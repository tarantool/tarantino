local server = require('tarantino.request')
local avro = require('avro_schema')
local t_schema = require('tarantino.schema')

function read(request)
     return server:request('read', request)
end

function insert(request, data)
     return server:request('insert', request, data)
end

function update(request, data)
     return server:request('update', request, data)
end

function delete(request)
     return server:request('delete', request)
end

function refresh(path)
    return server:refresh(path)
end
function health_check()
    return server:health()
end

return {
    create_index = function(self, space, schema)
        local parts = {}
        local names = avro.get_names(schema.pure)
        local types = avro.get_types(schema.pure)

        for _, part in pairs(server.config.index[space]) do
            self:add_index_part(parts, part, names, types)
        end

        box.space[space]:create_index('primary', {type='tree', parts=parts})
        log.info('Created index for space "%s"', space)
    end,
    init = function(self, replica)
        box.cfg{
            listen=server.config.port,
            slab_alloc_arena=server.config.memory,
            --replication_source=server.config.replication
        }
        local first = server.config.base
        if first == nil then
            return
        end
        log.info('Base version: "%s"', first)
        -- create spaces only in one instance (replication)
        if replica == nil or not replica then
            for name, schema in pairs(server.config.schema[first]) do
                if box.space[name] == nil then
                    box.schema.create_space(name)
                    log.info('Created space "%s"', name)
                    self:create_index(name, schema)
                end
            end
        end
    end,
    add_index_part = function(self, parts, part, names, types)
        for i, name in pairs(names) do
            if name == part then
                local index_type = types[i]
                table.insert(parts, i + 1)

                local tnt_type = 'unsigned'
                if index_type == 'string' then
                    tnt_type = 'str'
                end
                table.insert(parts, tnt_type)
                break
            end
        end
    end,
    start = function(self, path, port, replica)
        server:start(path, port)
	self:init(replica)
        box.once("user_grant", function()
            box.schema.user.grant('guest', 'read,write,execute','universe')
            box.schema.user.grant('guest','execute','role','replication')
        end)
        require('jit.opt').start('maxtrace=65000')
        require('jit.opt').start('maxmcode=2000')
        log.info('Tarantino start complete')
    end
}
