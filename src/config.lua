local log = require('log')
local schema = require('tarantino.schema')
local json = require('json')
local fio = require('fio')

return {
    config = {schema = {}},
    load_file = function(self, path)
        local file = fio.open(path)
        local buf = {}
        local i = 1
        while true do
            buf[i] = file:read(1024)
            if buf[i] == '' then
                break
            end
            i = i + 1
        end
        file:close()
        return table.concat(buf)
    end,

    load_config = function(self, path)
        local data = {}
        local ok, err = pcall(function()
            data = json.decode(self:load_file(path))
        end)
        if ok == false then
            -- split tarantool error message end return only text
            log.error(string.format(
                '%s:%s', conf, schema:split(err, ':')[3]
            ))
            os.exit(1)
        end
        for name, val in pairs(data) do
            self.config[name] = val
        end
    end,

    create_schema = function(self)
        local max_ver = 0
        local min_ver = nil
        for version, data in pairs(self.config.api) do
            local cmp_ver = tonumber(version)
            if cmp_ver == nil then
                log.error('Version parsing error (must be integer): "%s"', version)
                os.exit(1)
            end
            if min_ver == nil or cmp_ver < min_ver then
                min_ver = cmp_ver
            end
            if cmp_ver > max_ver then
                max_ver = cmp_ver
            end
            for sch_name, fields in pairs(data) do
                local ok, schema = avro.create(fields)
                if ok == false then
                    log.error('Class validation failed: "%s"', schema)
                    os.exit(1)
                end
                local ok, compiled = avro.compile(
                    {schema, service_fields={'string'}}
                )
                if ok == false then
                    log.error(
                        'Class compilation failed: "%s"', compiled
                    )
                    os.exit(1)
                end
                if self.config.schema[version] == nil then
                    self.config.schema[version] = {}
                end
                self.config.schema[version][sch_name] = {
                    pure = schema,
                    compiled = compiled,
                    cmp = cmp_ver
                }
            end
        end
        for version, data in pairs(self.config.api) do
            for sch_name, fields in pairs(data) do
                self.config.schema[version][sch_name].max_ver = max_ver
            end
        end
        self.config.base = tostring(min_ver)
    end,

    init = function(self, path, port)
        self.config.port = port
        self:load_config(path)
        self:create_schema()
        return self.config
    end
}
