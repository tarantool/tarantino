local SUCCESS=200
local FORBIDDEN=403
local NOT_ALLOWED=405
local BAD_REQUEST=400
local INTERNAL_ERR=500
local NOT_FOUND=404

local HTTP = {}
local text = {}
local codes = {
    SUCCESS=SUCCESS,
    FORBIDDEN=FORBIDDEN,
    NOT_ALLOWED=NOT_ALLOWED,
    BAD_REQUEST=BAD_REQUEST,
    INTERNAL_ERR=INTERNAL_ERR,
    NOT_FOUND=NOT_FOUND,
}

text[SUCCESS] = 'OK'
text[FORBIDDEN] = 'Forbidden'
text[NOT_ALLOWED] = 'Method is not allowed'
text[NOT_FOUND] = 'Not found'
text[BAD_REQUEST] = 'Bad request'
text[INTERNAL_ERR] = 'Internall server error'

for _, code in pairs(codes) do
    HTTP[code] = {
        status = {
            text=text[code],
            code=code
        }
    }
end

local get = {}
setmetatable(get, {
    __index = function(self, id)
        return HTTP[codes[id]]
    end
})

return {
    codes = codes,
    HTTP = get,
    by_code = HTTP
}
