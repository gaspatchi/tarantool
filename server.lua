json = require("json")
crypto = require("crypto")
digest = require("digest")
queue = require("queue")
fiber = require("fiber")
expirationd = require("expirationd")

box.cfg {
	listen = 3311,
	log= "gtmau.log"
}

config = {
	server={
		salt = os.environ()['password_salt']
	}
}

users_space = box.space.users
images_space = box.space.images
billing_space = box.space.billing
verifications_space = box.space.verifications
tokens_space = box.space.tokens
user_send_history = box.space.send_history
user_send_delay = box.space.send_delay
feedbacks_space = box.space.feedbacks
user_schedule = box.space.user_schedule
send_templates = box.space.templates

send_queue = queue.tube.send_queue
schedule_queue = queue.tube.schedule_queue
fetch_queue = queue.tube.fetch_queue
check_queue = queue.tube.check_queue

local function once()
	local users_space = box.schema.space.create("users")
	users_space:create_index("primary", {type = "tree", unique = true, parts = {1, "string"}})
	local images_space = box.schema.space.create("images")
	images_space:create_index("primary", {type = "tree", unique = true, parts = {1, "unsigned"}})
	local verifications_space = box.schema.space.create("verifications")
	verifications_space:create_index("primary", {type = "tree", unique = true, parts = {1, "string"}})
	local tokens_space = box.schema.space.create("tokens")
	tokens_space:create_index("primary", {type = "tree", unique = true, parts = {1, "string"}})
	tokens_space:create_index("secondary", {type = "tree", unique = false, parts = {2, "string"}})
	local billing_space = box.schema.space.create("billing")
	billing_space:create_index("primary", {type = "tree", unique = true, parts = {1, "string"}})

	local user_send_history = box.schema.space.create("send_history")
	user_send_history:create_index("primary", {type = "tree", unique = true, parts = {1, "string"}})

	local user_send_delay = box.schema.space.create("send_delay")
	user_send_delay:create_index("primary", {type = "tree", unique = true, parts = {1, "string"}})

	local user_schedule = box.schema.space.create("user_schedule")
	user_schedule:create_index("primary", {type = "tree", unique = true, parts = {1, "string"}})

	local feedbacks_space = box.schema.space.create("feedbacks")
	feedbacks_space:create_index("primary", {type = "tree", unique = true, parts = {1, "unsigned"}})
	feedbacks_space:create_index("secondary", {type = "tree", unique = false, parts = {2, "string"}})

	local send_templates = box.schema.space.create("templates")
	send_templates:create_index("primary", {type = "tree", unique = true, parts = {1, "string"}})

	queue.create_tube("schedule_queue", "fifottl", {temporary = true})
	queue.create_tube("send_queue", "fifottl", {temporary = true})
	queue.create_tube("fetch_queue", "fifottl", {temporary = true})
	queue.create_tube("check_queue", "fifottl", {temporary = true})

	box.schema.user.create("gtmau", {password = "zheserver3"})
	box.schema.user.grant("gtmau", "read, write, create, drop, alter, execute", "universe")
end

box.once("gtmau-1", once)

-- Получение каналов рассылки
function getDispatch(user_id)
	local profile = users_space:get(user_id)
	if profile ~= null then
		profile = json.decode(profile[2])
		return true, json.encode({email = profile.subscription.dispatch.email, sms = profile.subscription.dispatch.sms})
	else
		return false, "Пользователь не найден"
	end
end

-- Обновление каналов рассылки
function updateDispatch(user_id,email,sms)
	local profile = users_space:get(user_id)
	if profile ~= null then
		profile = json.decode(profile[2])
		profile.subscription.dispatch.email = email
		profile.subscription.dispatch.sms = sms
		users_space:update(user_id,{{"=",2,json.encode(profile)}})
		return true, "Каналы рассылки успешно обновлены"
	else
		return false, "Пользователь не найден"
	end
end

-- Получение активных подписок
function selectSubscriptions(user_id)
	local profile = users_space:get(user_id)
	if profile ~= null then
		profile = json.decode(profile[2])
		return true, profile.subscription.schedule
	else
		return false, "Пользователь не найден"
	end
end

-- Добавление подписки
function addSubscriptions(user_id, section, destination, type, id)
	local profile = users_space:get(user_id)
	if profile ~= null then
		profile = json.decode(profile[2])
		if section == "send" then
			if destination == "sms" then
				if type == "group" then
					for key,value in ipairs(profile.subscription.schedule.send.sms.groups) do
						if value == id then
							return true, "Группа была добавлена ранее"
						end
					end
					for key,value in ipairs(profile.subscription.schedule.view.groups) do
						if value == id then
							table.remove(profile.subscription.schedule.view.groups,key)
						end
					end
					table.insert(profile.subscription.schedule.send.sms.groups,id)
					table.sort(profile.subscription.schedule.send.sms.groups)
					users_space:update(user_id,{{"=",2,json.encode(profile)}})
					return true, "Группа успешно добавлена"
				elseif type == "teacher" then
					for key,value in ipairs(profile.subscription.schedule.send.sms.teachers) do
						if value == id then
							return true, "Преподаватель уже был добавлен ранее"
						end
					end
					for key,value in ipairs(profile.subscription.schedule.view.teachers) do
						if value == id then
							table.remove(profile.subscription.schedule.view.teachers, key)
						end
					end
					table.insert(profile.subscription.schedule.send.sms.teachers,id)
					table.sort(profile.subscription.schedule.send.sms.teachers)
					users_space:update(user_id,{{"=",2,json.encode(profile)}})
					return true, "Преподаватель успешно добавлен"
				else
					return false, "Данный тип рассылки недоступен"
				end
			elseif destination == "email" then
				if type == "group" then
					for key,value in ipairs(profile.subscription.schedule.send.email.groups) do
						if value == id then
							return true, "Группа уже была добавлена ранее"
						end
					end
					for key,value in ipairs(profile.subscription.schedule.view.groups) do
						if value == id then
							table.remove(profile.subscription.schedule.view.groups,key)
						end
					end
					table.insert(profile.subscription.schedule.send.email.groups,id)
					table.sort(profile.subscription.schedule.send.email.groups)
					users_space:update(user_id,{{"=",2,json.encode(profile)}})
					return true, "Группа успешно добавлена"
				elseif type == "teacher" then
					for key,value in ipairs(profile.subscription.schedule.send.email.teachers) do
						if value == id then
							return true, "Преподаватель уже был добавлен ранее"
						end
					end
					for key,value in ipairs(profile.subscription.schedule.view.teachers) do
						if value == id then
							table.remove(profile.subscription.schedule.view.teachers,key)
						end
					end
					table.insert(profile.subscription.schedule.send.email.teachers,id)
					table.sort(profile.subscription.schedule.send.email.teachers)
					users_space:update(user_id,{{"=",2,json.encode(profile)}})
					return true, "Преподаватель успешно добавлен"
				else
					return false, "Данный тип рассылки недоступен"
				end
			else
				return false, "Данный метод рассылки недоступен"
			end
		end

		if section == "view" then
			if type == "group" then
				for key, value in ipairs(profile.subscription.schedule.view.groups) do
					if value == id then
						return true, "Группа уже была добавлена ранее"
					end
				end
				table.insert(profile.subscription.schedule.view.groups,id)
				table.sort(profile.subscription.schedule.view.groups)
				users_space:update(user_id,{{"=",2,json.encode(profile)}})
				return true, "Группа успешно добавлена"
			elseif type == "teacher" then
				for key, value in ipairs(profile.subscription.schedule.view.teachers) do
					if value == id then
						return true, "Преподаватель уже был добавлен ранее"
					end
				end
				table.insert(profile.subscription.schedule.view.teachers,id)
				table.sort(profile.subscription.schedule.view.teachers)
				users_space:update(user_id,{{"=",2,json.encode(profile)}})
				return true, "Преподаватель успешно добавлен"
			else
				return false, "Данный тип рассылки недоступен"
			end
		end
	else
		return false, "Пользователь не найден"
	end
end

function deleteSubscription(user_id, section, destination, type, id)
	local profile = users_space:get(user_id)
	if profile ~= null then
		profile = json.decode(profile[2])
		if type == "group" then
			for k,v in ipairs(profile.subscription.schedule.groups) do
				if v == id then
					table.remove(profile.subscription.schedule.groups,k)
				end
			end
			table.sort(profile.subscription.schedule.groups)
			users_space:update(user_id,{{"=",2,json.encode(profile)}})
			return true, "Группа успешно удалена"
		end
		if type == "teacher" then
			for k,v in ipairs(profile.subscription.schedule.teachers) do
				if v == id then
					table.remove(profile.subscription.schedule.teachers,k)
				end
			end
			table.sort(profile.subscription.schedule.teachers)
			users_space:update(user_id,{{"=",2,json.encode(profile)}})
			return true, "Преподаватель успешно удален"
		end
	else
		return false, "Пользователь не найден"
	end
end

-- Удаление подписки
function deleteSubscriptions(user_id, section, destination, type, id)
	local profile = users_space:get(user_id)
	if profile ~= null then
		profile = json.decode(profile[2])
		if section == "send" then
			if destination == "sms" then
				if type == "group" then
					for key,value in ipairs(profile.subscription.schedule.send.sms.groups) do
						if value == id then
							table.remove(profile.subscription.schedule.send.sms.groups, key)
							return true, "Группа уже была добавлена ранее"
						end
					end
					table.sort(profile.subscription.schedule.send.sms.groups)
					users_space:update(user_id,{{"=",2,json.encode(profile)}})
					return true, "Группа успешно удалена"
				elseif type == "teacher" then
					for key,value in ipairs(profile.subscription.schedule.send.sms.teachers) do
						if value == id then
							 table.remove(profile.subscription.schedule.send.sms.teachers, key)
						end
					end
					table.sort(profile.subscription.schedule.send.sms.teachers)
					users_space:update(user_id,{{"=",2,json.encode(profile)}})
					return true, "Преподаватель успешно удален"
				else
					return false, "Данный тип рассылки недоступен"
				end
			elseif destination == "email" then
				if type == "group" then
					for key,value in ipairs(profile.subscription.schedule.send.email.groups) do
						if value == id then
							table.remove(profile.subscription.schedule.send.email.groups,key)
						end
					end
					table.sort(profile.subscription.schedule.send.email.groups)
					users_space:update(user_id,{{"=",2,json.encode(profile)}})
					return true, "Группа успешно удалена"
				elseif type == "teacher" then
					for key,value in ipairs(profile.subscription.schedule.send.email.teachers) do
						if value == id then
							table.remove(profile.subscription.schedule.send.email.teachers,key)
						end
					end
					table.sort(profile.subscription.schedule.send.email.teachers)
					users_space:update(user_id,{{"=",2,json.encode(profile)}})
					return true, "Преподаватель успешно удален"
				else
					return false, "Данный тип рассылки недоступен"
				end
			else
				return false, "Данный метод рассылки недоступен"
			end
		end

		if section == "view" then
			if type == "group" then
				for key, value in ipairs(profile.subscription.schedule.view.groups) do
					if value == id then
						table.remove(profile.subscription.schedule.view.groups,key)
					end
				end
				table.sort(profile.subscription.schedule.view.groups)
				users_space:update(user_id,{{"=",2,json.encode(profile)}})
				return true, "Группа успешно удалена"
			elseif type == "teacher" then
				for key, value in ipairs(profile.subscription.schedule.view.teachers) do
					if value == id then
						table.remove(profile.subscription.schedule.view.teachers,key)
					end
				end
				table.sort(profile.subscription.schedule.view.teachers)
				users_space:update(user_id,{{"=",2,json.encode(profile)}})
				return true, "Преподаватель успешно удален"
			else
				return false, "Данный тип рассылки недоступен"
			end
		end
	else
		return false, "Пользователь не найден"
	end
end

-- Создание токена для отправки
function createVerification(user_id, action, payload)
	local token = digest.sha1_hex(digest.sha256_hex(digest.urandom(30)))
	verifications_space:insert({token,user_id,action,payload,os.time()})
	return token
end

-- Завершение верификации
function completeVerification(token)
	local result = verifications_space:get(token)
	if result ~= null then
		if result[3] == "verifyRegistration" then
			local profile = json.decode(users_space:get(result[2])[2])
			profile.info.meta.activated = true
			users_space:update(result[2],{{"=",2,json.encode(profile)}})
			verifications_space:delete(token)
			return true, "Успешное подтверждение регистрации аккаунта"
		end
		if result[3] == "resetPassword" then
			local profile = json.decode(users_space:get(result[2])[2])
			local pass = crypto.digest.sha512.new()
			pass:init()
			pass:update(result[4])
			pass:update(config.server.salt)
			profile.info.profile.password = pass:result()
			users_space:update(result[2],{{"=",2,json.encode(profile)}})
			verifications_space:delete(token)
			return true, "Успешное подтверждение сброса пароля"
		end
	else
		return false, "Токен не существует"
	end
end

-- Сброс пароля
function resetPassword(user_id)
	local profile = users_space:get(user_id)
	if profile ~= null then
		local newpassword = digest.base64_encode((digest.urandom(3)))
		local verification_token = createVerification(user_id,"resetPassword", newpassword)
		send_queue:put(json.encode({direction = "email", template = "resetPassword", data = {user_id = user_id, password = newpassword, token = verification_token}}),{pri=0,ttl=21600,ttr=40, delay = 1})
		return true, "Письмо с подтверждением сброса пароля отправлено на вашу почту"
	else
		return false, "Пользователя с такой почтой не существует"
	end
end

-- Получение изображения
function selectImage(image_id)
	local result = images_space:get(image_id)
	if result ~= null then
		return true, result[2]
	else
		return false
	end
end

-- Получение профиля
function selectUser(user_id)
	local profile = users_space:get(user_id)
	if profile ~= null then
		profile = json.decode(profile[2])
		local status, image = selectImage(profile.info.meta.image)
		if status == true then
			profile.info.meta.image = image
		else
			profile.info.meta.image = false
		end
		profile.info.profile.password = null
		profile.info.meta.activated = null
		return true, profile
	else
		return false, "Пользователь не найден"
	end
end

function selectBilling(user_id)
	local billing = billing_space:get(user_id)
	return true, billing[2]
end

-- Создание профиля пользователя
function registerUser(user_id,firstname,lastname,password)
	if users_space:get(user_id) == null then
		local pass = crypto.digest.sha512.new()
		pass:init()
		pass:update(password)
		pass:update(config.server.salt)
		local profile = {
			info = {
				meta = {type = "Студент", image = 1, activated=false},
				profile = {firstname = firstname, lastname = lastname, password = pass:result()},
				contacts = {email = user_id, number = false, push = {}}},
				subscription = {
					dispatch = {email = true, sms = false},
					schedule = {send = {sms = {teachers = {}, groups = {}}, email = {teachers = {}, groups = {}}}, view = {teachers = {}, groups = {}}}
				}
		}
		local billing = {
			balance = "10",
			flow = {
				{
				payment_id = 0,
				date = os.date("%Y-%m-%dT%H:%M:%SZ"),
				direction = "in",
				cause = "Регистрация аккаунта",
				amount = "10"
				}
			}
		}
		local send = {
			sms = {
				notset = os.time(),
				nomoney = os.time()
			}
		}
		users_space:insert({user_id,json.encode(profile),os.time()})
		billing_space:insert({user_id,json.encode(billing),os.time()})
		user_send_delay:insert({user_id,null,os.time()})
		user_schedule:insert({user_id,null,os.time()})
		local verification_token = createVerification(user_id,"verifyRegistration")
		send_queue:put(json.encode({direction = "email", template = "verifyRegistration", data = {user_id = user_id, token = verification_token}}),{pri = 40, ttl = 21600, ttr = 40, delay = 1})
		return true, "Пользователь успешно создан"
	else
		return false, "Пользователь уже существует"
	end
end

-- Редактирование профиля
function updateUser(user_id,firstname,lastname,number,image,password)
	local profile = users_space:get(user_id)
	if profile ~= null then
		profile = json.decode(profile[2])
		if (firstname ~= null and #firstname > 1) then
			profile.info.profile.firstname = firstname
		end
		if (lastname ~= null and #lastname > 1) then
			profile.info.profile.lastname = lastname
		end
		if (number ~= null and #number > 1) then
			profile.info.contacts.number = number
		end
		if (image ~= null and #image > 1) then
			if profile.info.meta.image == 1 then
				local image_id = images_space:auto_increment{image}
				profile.info.meta.image = image_id[1]
			else
				images_space:update(profile.info.meta.image,{{"=",2,image}})
			end
		end
		if (password ~= null and #password > 1) then
			local pass = crypto.digest.sha512.new()
			pass:init()
			pass:update(password)
			pass:update(config.server.salt)
			profile.info.profile.password = pass:result()
			deleteTokens(user_id)
		end
		users_space:update(user_id,{{"=",2,json.encode(profile)}})
		return true, "Профиль успешно обновлен"
	else
		return false, "Пользователь не найден"
	end
end

function deleteTokens(user_id)
	tokens = tokens_space.index.secondary:select(user_id)
	for _, token in ipairs(tokens) do
		tokens_space:delete(token[1])
	end
end

-- Валидация данных логина
function loginUser(user_id, password)
	local profile = users_space:get(user_id)
	local response = {}
	if profile ~= null then
		profile = json.decode(profile[2])
		local pass = crypto.digest.sha512.new()
		pass:init()
		pass:update(password)
		pass:update(config.server.salt)
		if profile.info.meta.activated == true then
			if pass:result() == profile.info.profile.password then
				response.type = profile.info.meta.type
				response.firstname = profile.info.profile.firstname
				response.lastname = profile.info.profile.lastname
				response.email = profile.info.contacts.email
				return true, response
			else
				return false, "Неверный пароль"
			end
		else
			 return false, "Аккаунт не активирован"
		end
	else
		return false, "Пользователь не найден"
	end
end

-- Проверка токена на существование
function existsToken(user_id, token)
	local exists = tokens_space:get(token)
	if exists ~= null then
		if exists[2] == user_id then
			return true
		else
			return false
		end
	else
		return false
	end
end

-- Сброс билллинговой информации
function resetbilling(user_id)
local billing = {
			balance = "10",
			flow = {
				{
				payment_id = 0,
				date = os.date("%Y-%m-%dT%H:%M:%SZ"),
				direction = "in",
				cause = "Регистрация аккаунта",
				amount = "10"
				}
			}
		}
	billing_space:update(user_id,{{"=",2,json.encode(billing)}})
end

-- Пополнение аккаунта пользователя
function refillUser(user_id, amount, payment_id)
	local exists = false
	local profile = users_space:get(user_id)
	if profile ~= null then
		local billing = billing_space:get(user_id)
		billing = json.decode(billing[2])
		for k,v in pairs(billing.flow) do
			if v.payment_id == payment_id then
				exists = true
			end
		end
		if exists == false then
			local numamount = tonumber(amount)/100
			local stramount = tostring(numamount)
			local balance = tostring(tonumber(billing.balance) + numamount)
			local payment = {payment_id = payment_id, date = os.date("%Y-%m-%dT%H:%M:%SZ"), direction = "in", cause = "Пополенение аккаунта",amount = stramount}
			table.insert(billing.flow,payment)
			billing.balance = balance
			billing_space:update(user_id,{{"=",2,json.encode(billing)}})
		end
		return true
	else
		return false
	end
end

-- Добавление токена в список
function putToken(user_id, token)
	tokens_space:replace{token,user_id,os.time()}
	return true
end

-- Информация для генерации токена
function createToken(user_id)
	local profile = users_space:get(user_id)
	local result = {}
	if profile ~= null then
		profile = json.decode(profile[2])
		result.type = profile.info.meta.type
		result.firstname = profile.info.profile.firstname
		result.lastname = profile.info.profile.lastname
		result.email = profile.info.contacts.email
		return json.encode(result)
	else
		return false
	end
end

-- Создание задачи на рассылку расписания
function postSchedule(group, teacher, date, action)
	schedule_queue:put(json.encode({action = action, group = group, teacher = teacher, date = date}),{pri=0,ttl=21600,ttr=40,delay=1})
	return true
end

-- Просмотр очереди с расписанием
function listenScheduleQueue()
	while true do
		local task = schedule_queue:take(0)
		local users = users_space:select()
		if task ~= null then
			for _, user in ipairs(users) do
				createSubscriptionTask(task, user)
			end
			schedule_queue:ack(task[1])
		end
		fiber.sleep(5)
	end
end

-- Создание задачи по рассылке расписания
function createSubscriptionTask(task, user)
	local send_history = user_send_history:get(user[1])
	local task_data = json.decode(task[3])
	local profile = json.decode(user[2])

	local subscribe_exists = false
	local sms_exists = false
	local email_exists = false
	local history_data = {}

	if send_history ~= null then
		history_data = json.decode(send_history[2])
	else
		history_data = {create = {email = {teachers = {},groups = {}},sms = {teachers = {},groups = {}},push = {}},
			update = {email = {teachers = {},groups = {}},sms = {teachers = {},groups = {}},push = {}}}
	end

	for _, destination in pairs({"sms","email"}) do
		for _, type in pairs({"teachers","groups"}) do
			
			local type_alias = ""
			if type == "teachers" then
				type_alias = "teacher"
			else
				type_alias = "group"
			end
			for key, value in ipairs(profile.subscription.schedule.send[destination][type]) do
				if value == task_data[type_alias] then
					local exists, update = checkDuplicates(history_data, task_data.action, destination, type, value)
					if update == true then
						subscribe_exists = true
						if destination == "sms" then
							sms_exists = true
						else
							email_exists = true
						end

						for index, number in ipairs(history_data[task_data.action][destination][type]) do
							if number.id == task_data[type_alias] then
								history_data[task_data.action][destination][type][index].time = os.time()
							end
						end
						fetch_queue:put(json.encode({action = task_data.action, destination = destination, type = type_alias, date = task_data.date, id = task_data[type_alias], data = {try = 0, user_id = user[1]}}),{pri = 0, ttl = 21600, ttr = 40, delay = 5})
					end

					if exists == false then
						subscribe_exists = true
						if destination == "sms" then
							sms_exists = true
						else
							email_exists = true
						end
						table.insert(history_data[task_data.action][destination][type], {id = task_data[type_alias], time = os.time()})
						fetch_queue:put(json.encode({action = task_data.action, destination = destination, type = type_alias, date = task_data.date, id = task_data[type_alias], data = {try = 0, user_id = user[1]}}),{pri = 0, ttl = 21600, ttr = 40, delay = 5})
					end
				end
				user_send_history:upsert({user[1], json.encode(history_data),os.time()}, {{"=",2,json.encode(history_data)}})
			end
		end
	end

	local template_alias = ""
	if task_data.action == "create" then
		template_alias = "createSchedule"
	else
		template_alias = "updateSchedule"
	end

	if sms_exists == true and profile.subscription.dispatch.sms == true then
		sendSms(user, profile, task_data.action)
	end

	if email_exists == true and profile.subscription.dispatch.email == true then
		send_queue:put(json.encode({direction = "email", date = task_data.date, template = template_alias, data = {try = 0, user_id = user[1]}}),{pri = 0, ttl = 21600, ttr = 40, delay = 200})
	end

	if subscribe_exists == true and #profile.info.contacts.push ~= 0 then
		if #history_data[task_data.action].push > 0 and (os.time() - history_data[task_data.action].push.time) > 10 then
			history_data[task_data.action].push.state = "onsend"
			history_data[task_data.action].push.time = os.time()
			send_queue:put(json.encode({direction = "push", template = "push" .. template_alias, data = {try = 0, user_id = user[1]}}),{pri = 2, ttl = 21600, ttr = 40, delay = 1})
		elseif #history_data[task_data.action].push == 0 then
			history_data[task_data.action].push = {state = "onsend", time = os.time()}
			send_queue:put(json.encode({direction = "push", template = "push" .. template_alias, data = {try = 0, user_id = user[1]}}),{pri = 2, ttl = 21600, ttr = 40, delay = 1})
		end
	end
	user_send_history:upsert({user[1],json.encode(history_data)}, {{"=",2,json.encode(history_data)}})
end		

function selectSchedule(user_id, direction, date, action)
	local schedule = user_schedule:get(user_id)
	if schedule ~= null then
		local action_alias = ""
		if action == "createSchedule" then
			action_alias = "create"
		elseif action == "updateSchedule" then
			action_alias = "update"
		else
			return false, "Такого типа не существует"
		end
		schedule = json.decode(schedule[2])
		for key, value in ipairs(schedule) do
			if value.date == date then
				if schedule[key].schedule[action_alias][direction].groups == null and schedule[key].schedule[action_alias][direction].teachers == null then
					return false, "Расписание отсутсвует"
				else
					return true, json.encode(schedule[key].schedule[action_alias][direction])
				end
			end
		end
	else
		return false, "Пользователь не найден"
	end
end

function checkDuplicates(history_data, action, destination, type, id)
	local update = false
	local exists = false
	for key, value in ipairs(history_data[action][destination][type]) do
		if value.id == id then
			if action == "update" and (os.time() - value.time) > 10 then 
				update = true
			end
			exists = true
		end
	end
	return exists, update
end

function sendSms(user, profile, action)
	local send_delay = user_send_delay:get(user[1])
	local _, billing = selectBilling(user[1])
	local send_delay_data = json.decode(send_delay[2])

	local template_alias = ""
	if action == "create" then
		template_alias = "createSchedule"
	else
		template_alias = "updateSchedule"
	end

	if profile.info.contacts.number == false then
		if send_delay_data.sms.notset ~= false and (os.time() - send_delay_data.sms.notset) > 20 then
			send_queue:put(json.encode({direction = "email", template = "setNumber", data = {try = 0, user_id = user[1]}}),{pri = 4, ttl = 240, ttr = 40, delay = 1})
			send_delay_data.sms.notset = os.time()
			user_send_delay:update(user[1],{{"=", 2, json.encode(send_delay_data)}})
		end
	end

	if profile.info.contacts.number ~= false then
		if send_delay_data.sms.notset ~= false and (os.time() - send_delay_data.sms.nomoney) > 20 then
			local balance = (tonumber(billing.balance) - 2) -- смс стоит 2 рубля	
			if balance > 0 then
				send_queue:put(json.encode({direction = "sms", template = "sms" .. template_alias, data = {try = 0, user_id = user[1]}}),{pri = 0, ttl = 21600, ttr = 40, delay = 1})
			else
				send_queue:put(json.encode({direction = "email", template = "addMoney", data = {try = 0, user_id = user[1]}}),{pri = 4, ttl = 240, ttr = 40, delay = 1})
				send_delay_data.sms.nomoney = os.time()
				user_send_delay:update(user[1],{{"=",2,json.encode(send_delay_data)}})
			end
		end
	end
end

function postForm(type, firstname, lastname, number, email, text)
	local type_alias
	if type == "feedback" then
		type_alias = "Обратная связь"
	elseif type == "driving" then
		type_alias = "Курсы вождения"
	elseif type == "hairdresser" then
		type_alias = "Курсы парикмахера"
	elseif type == "florist" then
		type_alias = "Курсы флориста"
	else
		type_alias = type
	end
	local form = {firstname = firstname, lastname = lastname, number = number, email = email, text = text}
	local feedback = feedbacks_space:auto_increment{type, json.encode(form)}
	send_queue:put(json.encode({direction = "email", template = type, data = {try = 0, address = "info@xn----etbgb7bzaw.xn--p1ai", firstname = firstname, lastname = lastname, number = number, email = email, type = type_alias, text = text}}),{pri = 0, ttl = 86400, ttr = 40, delay = 1})
	return feedback[1]
end

function postFormCount(type)
	return feedbacks_space.index.secondary:count(type)
end

function getAddresses(user_id)
	local user = users_space:get(user_id)
	if user ~= null then
		local profile = json.decode(user[2])
		if profile.info.contacts.number ~= false then
			return true, profile.info.contacts.email, profile.info.contacts.number
		else
			return true, profile.info.contacts.email, ""
		end
	else
		return false, "Пользователь не найден"
	end
end

function getInitials(user_id)
	local user = users_space:get(user_id)
	if user ~= null then
		local profile = json.decode(user[2])
		return true, profile.info.profile.firstname, profile.info.profile.lastname
	else
		return false, "Пользователь не найден"
	end
end

function getTemplate(template_id)
	local template = send_templates:get(template_id)
	if template ~= null then
		return true, template[2]
	else
		return false, "Шаблон не найден"
	end
end

function formatSchedule(user_id,date,action,destination,type,newschedule)
	local schedule = user_schedule:get(user_id)
	if schedule ~= null then
		local newschedule = json.decode(newschedule)
		local exists = false
		local exists_entry = false
		local info_alias = ""
		local type_alias = ""
		if type == "teacher" then
			info_alias = "teacher_id"
			type_alias = "teachers"
		else
			info_alias = "group_id"
			type_alias = "groups"
		end

		if schedule[2] == null then
			schedule = {}
			table.insert(schedule,{date = date, schedule = {create = {email = {teachers = {},groups = {}},sms = {teachers = {},groups = {}}},update = {email = {teachers = {},groups = {}},sms = {teachers = {},groups = {}}}}})
		else
			schedule = json.decode(schedule[2])
		end

		for key, value in ipairs(schedule) do 
			if value.date == date then
				exists = true
				if #value.schedule[action][destination][type_alias] == 0 then
					table.insert(schedule[key].schedule[action][destination][type_alias],newschedule)
					exists_entry = true
				else
					for k,v in ipairs(value.schedule[action][destination][type_alias]) do
						if v.info.result[type][info_alias] == newschedule.info.result[type][info_alias] then
							exists_entry = true
							schedule[key].schedule[action][destination][type_alias][k] = newschedule
						end
					end
				end
			end
		end

		if exists == false then
			table.insert(schedule,{date = date,schedule = {create = {email = {teachers = {},groups = {}},sms = {teachers = {},groups = {}}},update = {email = {teachers = {},groups = {}},sms = {teachers = {},groups = {}}}}})
			for key, value in ipairs(schedule) do 
				if value.date == date then
					exists_entry = true
					table.insert(schedule[key].schedule[action][destination][type_alias], newschedule)
				end
			end
		end

		if exists_entry == false then
			for key, value in ipairs(schedule) do 
				if value.date == date then
					table.insert(schedule[key].schedule[action][destination][type_alias], newschedule)
				end
			end
		end

		user_schedule:update(user_id,{{"=",2,json.encode(schedule)}})
		return true
	else
		return false, "Пользователь не найден"
	end
end

fiber.create(listenScheduleQueue)

function is_expired(args, tuple)
    if args.type == "clean_inactive_users" then
        local profile = json.decode(tuple[2])
        if (os.time() - tuple[3] >= 21600 and profile.info.meta.activated == false) then
            return true
        end
    end
    if args.type == "clean_unconfirmed_tokens" then
        if (os.time() - tuple[5] >= 21600) then
            return true
        end
    end
    if args.type == "delete_expired_tokens" then
        if (os.time() - tuple[3] >= 1209600) then
            return true
        end
    end
    if args.type == "delete_sent_schedule" then
        if (os.time() - tuple[3] >= 18000) then
            return true
        end
    end
    if args.type == "delete_schedule" then
        if (os.time() - tuple[3] >= 18000) then
            return true
        end
    end
end

function deleteExpiredSchedule(space_id, args, tuple)
	box.space[space_id]:delete(tuple[1])
	box.space[space_id]:insert({tuple[1],null,os.time()})
end

-- Задача на удаление неактивированых юзеров
expirationd.start("clean_inactive_users", users_space.id, is_expired, {
    process_expired_tuple = nil, args = {type="clean_inactive_users"},
    tuples_per_iteration = 100, full_scan_time = 30
})

-- Задача на удаление неподтвержденных действий
expirationd.start("clean_unconfirmed_tokens", verifications_space.id, is_expired, {
    process_expired_tuple = nil, args = {type="clean_unconfirmed_tokens"},
    tuples_per_iteration = 100, full_scan_time = 30
})

-- Задача на удаление устаревших токенов
expirationd.start("delete_expired_tokens", tokens_space.id, is_expired, {
    process_expired_tuple = nil, args = {type="delete_expired_tokens"},
    tuples_per_iteration = 100, full_scan_time = 30
})

-- Задача на удаление истории разосланного расписания
expirationd.start("delete_sent_schedule", user_send_history.id, is_expired, {
    process_expired_tuple = nil, args = {type="delete_sent_schedule"},
    tuples_per_iteration = 100, full_scan_time = 30
})

-- Задача на удаление разосланного расписания
expirationd.start("delete_schedule", user_schedule.id, is_expired, {
    process_expired_tuple = deleteExpiredSchedule, args = {type="delete_schedule"},
    tuples_per_iteration = 100, full_scan_time = 30
})
