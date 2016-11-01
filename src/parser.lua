local schema = require('tarantino.schema')

return {
    parse_index = function(self, vals, space)
        local parts = box.space[space].index[0].parts
        local result = {}
        if #vals > #parts then
            return
        end
        for i, val in pairs(vals) do
            if parts[i].type ~= 'string' then
                local num = tonumber(val)
                if num == nil then
                    return
                end
                table.insert(result, num)
            end
        end
        return result
    end,

    find_space = function(self, request, config)
        local uri = schema:split(request.uri, '?')[1]
        local params = schema:split(uri, '/')
        -- /
        table.remove(params, 1)
        -- entry
        local entry = params[1]
        table.remove(params, 1)
        -- version
        local version = params[1]
        table.remove(params, 1)
        if config.schema[version] == nil then
            return
        end
        -- space
        local space = params[1]
        table.remove(params, 1)
        if box.space[space] == nil then
            return
        end

        local index = self:parse_index(params, space)
        if index == nil then
            return
        end

        local limit = tonumber(request.args.limit)
        local offset = request.args.offset
        if limit == nil then
            limit = 100
        end

        return {
            version = version,
            entry = entry,
            index = index,
            space = space,
            limit = limit,
            offset = offset
        }
    end
}
