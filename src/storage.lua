local sh = require('tarantino.schema')
local log = require('log')

return {
    actualize = function(self, parsed, config)
        local schema = config.schema[parsed.version][parsed.space]
        local max_ver = schema.max_ver
        local max_tag = tostring(max_ver)

        if schema.cmp ~= max_ver then
            local stored_schema = config.schema[max_tag][parsed.space]
            version = max_tag
            schema = sh:transform(schema, stored_schema)
        end
        return schema, version
    end,
    decompress = function(self, tuple, schema)
        local l = #tuple
        if l == 0 then
            return {}
        elseif l == 1 then
            tuple = tuple[1]
        end
        local ok, result, ver = schema.compiled.unflatten(tuple)
        return result, ver
    end,
    to_user_ver = function(self, result, parsed, config)
        local response = {}
        local res, ver  = nil,nil

        for _, tuple in pairs(result) do                              
            local schema = config.schema[parsed.version][parsed.space]
            if #tuple > 0 and tuple[1] ~= parsed.version then
                local stored_schema = config.schema[tuple[1]][parsed.space]
                schema = sh:transform(stored_schema, schema)  
            end 

            res, ver = self:decompress(tuple, schema)
            table.insert(response, res)
        end     
            
        -- full key request unwrap 
        if #result > 0 and
                #parsed.index == #box.space[parsed.space].index[0].parts then     
            return response[1], ver 
        end
        return response, ver
    end
}
