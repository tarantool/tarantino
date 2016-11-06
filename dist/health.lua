local nb = require('net.box')
local c = nb:new('127.0.0.1:3301')
local status = c:call('health_check')
if status == '' then
    os.exit(0)
end
print(status)
os.exit(1)
