-- Functions:
-- Implement plain controller with join
local config = require('tarantino.config')
local parser = require('tarantino.parser')
local const = require('tarantino.const')
local storage = require('tarantino.storage')
local log = require('log')
local sh = require('tarantino.schema')

return {
    read = function(self, request, parsed)
        local data = box.space[parsed.space]:select(
            parsed.index, {limit=parsed.limit, offset=parsed.offset}
        )
        if #data == 0 then
            return const.codes.NOT_FOUND
        end
        return storage:to_user_ver(data, parsed, self.config)
    end,
    insert = function(self, request, parsed, data)
        local schema, version = storage:actualize(parsed, self.config)
        local ok, new_tuple = schema.compiled.flatten(data, version)
        if not ok then
            return const.codes.BAD_REQUEST
        end
        box.begin()
        box.space[parsed.space]:insert(new_tuple)
        box.commit()
        return {}      
    end,
    update = function(self, request, parsed, u_data)
        if #parsed.index ~= #box.space[parsed.space].index[0].parts then
            return const.codes.BAD_REQUEST
        end
        local data, ver = self:read(request, parsed)
        if next(data) == nil then
            return const.codes.NOT_FOUND
        end
        local cur_schema = self.config.schema[parsed.version][parsed.space]
        local ok, old_tuple = cur_schema.compiled.flatten(
            data, ver
        )
        if not ok then
            return const.codes.BAD_REQUEST
        end

        local max_ver = tostring(cur_schema.max_ver)
        if ver ~= max_ver then
            local stored_schema = self.config.schema[max_ver][parsed.space]
            local trans = sh:transform(cur_schema, stored_schema)

            local json, ver = storage:decompress(old_tuple, trans)
            local ok, new_tuple = stored_schema.compiled.flatten(
                data, max_ver
            )
            if not ok then
                return const.codes.BAD_REQUEST
            end
            local tuple = box.tuple.new(new_tuple)
            local ok, fields = stored_schema.compiled.xflatten(u_data)
            if not ok then
                return const.codes.BAD_REQUEST
            end
            tuple = tuple:update(fields)
            box.begin()
            box.space[parsed.space]:replace(tuple)
            box.commit()
            return {}
        end
        local schema, version = storage:actualize(parsed, self.config)
        local ok, fields = schema.compiled.xflatten(u_data)
        if not ok then
            return const.codes.BAD_REQUEST
        end
        box.begin()
        box.space[parsed.space]:update(parsed.index, fields)
        box.commit()
        
        return {}
    end,
    delete = function(self, request, parsed)
        box.begin()
        box.space[parsed.space]:delete(parsed.index)
        box.commit()
        return {}
    end,

    auth = function(self, request, parsed)
        -- FIXME: implement simple token auth from config
	-- use HTTP header tr_token and comapre with config
        return true
    end,

    response = function(self, request, parsed, result)
        return {
            status = const.HTTP.SUCCESS.status,
            data = result,
            --meta = {debug=parsed}
        }
    end,

    request = function(self, func, request, data)
        local obj = parser:find_space(request, self.config)
        if obj == nil then
            return const.HTTP.BAD_REQUEST
        end
        if not self:auth(request, obj) then
            return const.HTTP.FORBIDDEN
        end
        local result = {}
        local ok, err = pcall(function()
            result = self[func](self, request, obj, data)
        end)
        if not ok then
            local ret = const.HTTP.INTERNAL_ERR
            ret['message'] = err
            return ret
        end
        -- avro errors handling
        if type(result) == 'string' then
            local ret = const.HTTP.INTERNAL_ERR
            ret['message'] = result
            return ret
        end
        -- handle internal errors
        if type(result) == 'number' then
            return const.by_code[result]
        end
        return self:response(request, obj, result)
    end,

    start = function(self, path, port)
        self.config = config:init(path, port)
    end
}
