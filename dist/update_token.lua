local nb = require('net.box')
local c = nb:new('127.0.0.1:3301')
if c:call('refresh', arg[1]) then
    os.exit(0)
end
os.exit(1)
