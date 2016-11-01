#!/usr/bin/env tarantool
local http = require('http.client')
local json = require('json')
local fiber = require('fiber')
local tap = require('tap')
local debug = true
local base = 'http://172.17.0.2'
function request(r_type, uri, body, headers)
    local header = {headers={}}
    if headers ~= nil then
        header = headers
    end
    local result = http.request(
        r_type, base .. uri,
        body, header
    )
    if debug then
        print(r_type)
        print(result.body)
    end
    return json.decode(result.body)
end

local test = tap.test("Tarantino API test")
test:plan(1)

test:test("Simple test", function(test)
    test:plan(11)
    local resp = request('delete', '/api/1/user/1')
    test:is(resp.status.code, 200, "Delete first")
    resp = request('delete', '/api/1/user/2')
    test:is(resp.status.code, 200, "Delete second")

    resp = request('put', '/api/1/user/1',
        '{"params":[{"uid":1,"first_name":"andrey","last_name":"drozdov"}]}'
    )
    test:is(resp.status.code, 200, "Put first")
    resp = request('put', '/api/1/user/2',
        '{"params":[{"uid":2,"first_name":"Andrey2","last_name":"Drozdov"}]}'
    )
    test:is(resp.status.code, 200, "Put second")

    resp = request('get', '/api/1/user/1')
    test:is(resp.status.code, 200, "Check first")
    resp = request('get', '/api/1/user/2')
    test:is(resp.status.code, 200, "Check second")
    resp = request('get', '/api/1/user')
    test:is(#resp.data, 2, "Check list query")
    resp = request('get', '/api/1/user?limit=1')
    test:is(#resp.data, 1, "Check limited list query")

    resp = request('post', '/api/1/user/2',
        '{"params":[{"last_name":"test update"}]}'
    )
    test:is(resp.status.code, 200, "Exec update")

    resp = request('get', '/api/1/user/2')
    test:is(resp.data.last_name, 'test update', "Check update request")

    resp = request('post', '/api/1/user/2',
        '{"params":[{"last_name":123}]}'
    )
    test:is(resp.status.code, 400, "Update with bad avro")
end)

os.exit()
