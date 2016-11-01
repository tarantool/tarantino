avro = require('avro_schema')
log = require('log')

return {
    transform_cache = {},
    schemas = {},
    init = function(self, config)
        local schema_conf = config.list().schema
        -- create avro schema from lua config for each version
        for domain, d in pairs(schema_conf) do
            for version, schema in pairs(d) do
                if self.schemas[domain] == nil then
                    self.schemas[domain] = {}
                end
                local ok, s = avro.create(schema)
                local cok, c = avro.compile(s)
                self.schemas[domain][version] = {pure=s, compiled=c}
            end
        end

    end,

    transform = function(self, schema_a, schema_b)
        -- try to get precompiled from cache
        local caches = self.transform_cache[schema_a]
        if caches ~= nil and caches[schema_b] ~= nil then
            return caches[schema_b]
        end

        -- compile transformation a->b
        local dg_flag = schema_a.cmp > schema_b.cmp
        local ok, transform = avro.compile({
            schema_a.pure, schema_b.pure, downgrade=dg_flag,
            service_fields={'string'}
        })
        if not ok then
            log.error(transform)
        end

        -- cache it
        if self.transform_cache[schema_a] == nil then
            self.transform_cache[schema_a] = {}
        end
        self.transform_cache[schema_a][schema_b] = {
            compiled=transform
        }
        return self.transform_cache[schema_a][schema_b]
    end,
    split = function(self, str, delim)
        if string.find(str, delim) == nil then
            return { str }
        end
        local result,pat,lastpos = {},"(.-)" .. delim .. "()",nil
        for part, pos in string.gfind(str, pat) do
            table.insert(result, part)
            lastpos = pos
        end
        table.insert(result, string.sub(str, lastpos))
        return result
    end,
    deepcopy = function(self, orig, ...)
        local orig_type = type(orig)
        local copy
        if orig_type == 'table' and not avro.is(orig) then
            copy = {}
            for orig_key, orig_value in next, orig, nil do
                local go_deep = true
                for i, exclude in pairs{...} do
                    if orig_key == exclude then
                        go_deep = false
                        break
                    end
                end
                if go_deep then
                    copy[self:deepcopy(orig_key)] = self:deepcopy(orig_value)
                end
            end
            setmetatable(copy, self.deepcopy(getmetatable(orig)))
        else -- number, string, boolean, etc
            copy = orig
        end
        return copy
    end
}
