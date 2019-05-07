local mysql = require "mysql"

local ORM = {}

function ORM:new()
    local t = {}
    for k, v in pairs(self) do
        t[k] = v
    end
    
    ORM._sql = {
        select = "",
        join   = "",
        where  = "",
        group  = "",
        order  = "",
    }

    return t
end

function ORM:init(db)
    self._orm_db = db
    return self
end

function ORM:table(tableName)
    self._tableName = tableName
    return self
end

function ORM:select(...)
    local selects = {...}
    for _, select in pairs(selects) do
        select = "`" .. string.gsub(select, "%.", "`.`") .. "`"
        
        ORM._sql.select = ORM._sql.select .. select .. ", "
    end
    return self
end

function ORM:selectRaw(...)
    local selects = {...}
    for _, select in pairs(selects) do
        ORM._sql.select = ORM._sql.select .. select .. ", "
    end
    return self
end

function ORM:join(joinsql)
    ORM._sql.join = ORM._sql.join .. "JOIN " .. joinsql .. " "
    return self
end

function ORM:leftJoin(joinsql)
    ORM._sql.join = ORM._sql.join .. "LEFT JOIN " .. joinsql .. " "
    return self
end

function ORM:where(where)
    for column, v in pairs(where) do
        column =  "`" .. string.gsub(column, "%.", "`.`") .. "`"
        ORM._sql.where = ORM._sql.where .. column .. " = " .. mysql.quote_sql_str(v) .. " AND "
    end
    return self
end

function ORM:whereRaw(...)
    local wheres = {...}
    for _, where in pairs(wheres) do
        ORM._sql.where = ORM._sql.where .. where .. " AND "
    end
    return self
end

function ORM:groupBy(...)
    local columns = {...}
    for _, column in pairs(columns) do
        column = "`" .. string.gsub(column, "%.", "`.`") .. "`"
        ORM._sql.group = ORM._sql.group .. column .. ", "
    end
    return self
end

function ORM:groupByRaw(...)
    local columns = {...}
    for _, column in pairs(columns) do
        ORM._sql.group = ORM._sql.group .. column .. ", "
    end
    return self
end

function ORM:orderBy(column, asc)
    if not asc then asc = "ASC" end
    column = "`" .. string.gsub(column, "%.", "`.`") .. "`"
    ORM._sql.order = ORM._sql.order .. column .. " " .. asc .. ", "
    return self
end

function ORM:orderByRaw(column, asc)
    if not asc then asc = "ASC" end
    ORM._sql.order = ORM._sql.order .. column .. " " .. asc .. ", "
    return self
end

function ORM:limit(limit)
    ORM._sql.limit = limit
    return self
end

function ORM:offset(offset)
    ORM._sql.offset = offset
    return self
end

function ORM:_lastSql()

    local select = ORM._sql.select
    if select == "" then
        select = "*"
    else
        select = string.sub(select, 1, string.len(select) - 2)
    end

    local join = ORM._sql.join
    join = string.sub(join, 1, string.len(join) - 1)

    local where = ORM._sql.where
    if where ~= "" then
        where = "WHERE " .. string.sub(where, 1, string.len(where) - 5)
    end

    local group = ORM._sql.group
    if group ~= "" then
        group = "GROUP BY " .. string.sub(group, 1, string.len(group) - 2)
    end

    local order = ORM._sql.order
    if order ~= "" then
        order = "ORDER BY " .. string.sub(order, 1, string.len(order) - 2)
    end

    local limit = ""
    if ORM._sql.limit then
        limit = "LIMIT " .. ORM._sql.limit
    end

    local offset = ""
    if ORM._sql.offset then
        offset = "OFFSET " .. ORM._sql.offset
    end

    return string.format("SELECT %s FROM `%s` %s %s %s %s %s %s", select, self._tableName, join, where, group, order, limit, offset) 
end

function ORM:beginTransaction()
    self._orm_db:transaction_begin()
end

function ORM:commit()
    self._orm_db:transaction_commit()
end

function ORM:rollback()
    self._orm_db:transaction_rollback()
end

function ORM:first()

    local function _setProperty(t, properties)
        for k, v in pairs(properties) do
            t[k] = v
        end
        return t
    end

    self:limit(1)
    local sql = self:_lastSql()
    local r = self._orm_db:assert_query(sql)
    local res
    if #r > 0 then
        res = self:new()
        res.init(self._orm_db)
        res._orm_old_property = r[1]
        _setProperty(res, r[1])
    end
    return res
end

function ORM:find(id)

    local function _primaryKey()
        for _, v in pairs(self._columns) do
            local tags = string.split(v, ";")
            for k, n in pairs(tags) do
                n = string.trim(n)
                if string.find(n, "column") == 1 then
                    local pos = string.find(n, ":")
                    return string.sub(n, pos + 1, string.len(n))
                end
            end
        end
        return "id"
    end

    local primaryKey = _primaryKey()
    self:where({[primaryKey] = id})
    self:limit(1)
    return self:first()
end

function ORM:get()

    local function _setProperty(t, properties)
        for k, v in pairs(properties) do
            t[k] = v
        end
        return t
    end

    local sql = self:_lastSql()
    local r = self._orm_db:assert_query(sql)

    local res = {}
    if r then
        local i = 0
        for k, v in pairs(r) do
            
            local tmp = self:new()
            tmp:init(self._orm_db)
            tmp._orm_old_property = v
            _setProperty(tmp, v)
            i = i+1
            res[i] = tmp
        end
    end
    return res
end

function ORM:count()

    self:selectRaw("COUNT(1) AS c")
    return self:first().c
end

function ORM:save()

    assert(self, "方法调用出错, 请用 \":save()\"")

    local function _primaryKey()
        local pks = {}
        for _, v in pairs(self._columns) do

            local tags = string.split(v, ";")

            local column = ""
            for k, n in pairs(tags) do
                n = string.trim(n)
                if string.find(n, "column") == 1 then
                    local pos = string.find(n, ":")
                    column = string.sub(n, pos + 1, string.len(n))
                end
            end

            if column == "id" then
                pks[column] = self[column]
            end

            for k, n in pairs(tags) do
                n = string.trim(n)
                if string.upper(n) == "PRIMARY_KEY" then
                    pks[column] = self[column]
                end
            end
        end
        return pks
    end

    local columns = {}
    for k, v in pairs(self) do
        if k ~= "_tableName" and (type(v) == "string" or type(v) == "number") then
            columns[k] = v
        end
    end

    if self._orm_old_property then

        local diff
        
        for k, v in pairs(columns) do
            for k2, v2 in pairs(self._orm_old_property) do
                if k == k2 and v ~= v2 then
                    if not diff then
                        diff = {}
                    end
                    diff[k] = v
                end
            end
        end

        if diff then
            return self._orm_db:update(self._tableName, diff, _primaryKey())
        end
        return 0
    end

    local sql = "INSERT INTO `" .. self._tableName .. "`("
    local values = " VALUES("

    local i = 0
    for k, v in pairs(columns) do
        if i ~= 0 then
            sql = sql .. ","
            values = values .. ","
        end
        sql = sql .. "`" .. k .. "`"
        values = values .. mysql.quote_sql_str(v)
        i = i+1
    end

    sql = sql .. ")" .. values .. ")"

    local res = self._orm_db.db:query(sql)
    assert(res.errno == nil, sql .. " " .. dump_tostring(res.err))
    local insert_id = res.insert_id

    local r = self:new()
    r.init(self._orm_db)
    if insert_id > 0 then
        r = r:find(insert_id)
    else
        r = r:where(_primaryKey()):first()
    end
    if r then
        for k, v in pairs(r) do
            self[k] = v
        end
    end

    self._orm_old_property = r
end

function ORM:delete()

    local function _primaryKey()
        local pks = {}
        for _, v in pairs(self._columns) do

            local tags = string.split(v, ";")

            local column = ""
            for k, n in pairs(tags) do
                n = string.trim(n)
                if string.find(n, "column") == 1 then
                    local pos = string.find(n, ":")
                    column = string.sub(n, pos + 1, string.len(n))
                end
            end

            if column == "id" then
                pks[column] = self[column]
            end

            for k, n in pairs(tags) do
                n = string.trim(n)
                if string.upper(n) == "PRIMARY_KEY" then
                    pks[column] = self[column]
                end
            end
        end
        return pks
    end

    local primaryKey = _primaryKey()

    local whereSql = ""
    local i = 0
    for k, v in pairs(primaryKey) do
        if i ~= 0 then
            whereSql = whereSql .. " AND "
        end
        whereSql = whereSql .. "`" .. k .. "` = " .. mysql.quote_sql_str(v)
        i = i+1
    end

    local sql = "DELETE FROM `" .. self._tableName .. "` WHERE " .. whereSql
    local res = self._orm_db.db:query(sql)

    log.debug("query[%s]", sql)
    assert(res.errno == nil, sql .. " " .. dump_tostring(res.err))
    return res.affected_rows
end

function ORM:scan()

    local primaryKeys = {}
    local types = {}
    local notNulls = {}
    local defaults = {}
    local uniques = {}
    local uniques2 = {}
    local indexs = {} -- ex: {index_name={column1, column2}}
    local comments = {}
    local auto_increments = {}

    for _, v in pairs(self._columns) do

        if v == "column:id" or v == "COLUMN:id" then
            v = "column:id;AUTO_INCREMENT"
        end

        local tags = string.split(v, ";")

        local column = ""
        for k, n in pairs(tags) do
            n = string.trim(n)
            if string.find(n, "column") == 1 then
                local pos = string.find(n, ":")
                column = string.sub(n, pos + 1, string.len(n))
            end
        end

        for k, n in pairs(tags) do
            n = string.trim(n)
            if string.upper(n) == "AUTO_INCREMENT" then
                auto_increments[column] = true
            end
            if string.upper(n) == "PRIMARY_KEY" then
                table.insert(primaryKeys, column)
            end
            if string.find(n, "type") == 1 then
                local pos = string.find(n, ":")
                types[column] = string.sub(n, pos + 1, string.len(n))
            end
            if string.upper(n) == "NOT NULL" then
                notNulls[column] = true
            end
            if string.find(n, "default") == 1 then
                local pos2 = string.find(n, ":")
                defaults[column] = string.sub(n, pos2 + 1, string.len(n))
            end
            if string.upper(n) == "UNIQUE" then
                uniques[column] = true
            else
                if string.find(n, "unique") == 1 then
                    local pos = string.find(n, ":")
                    local unique_index_name = string.sub(n, pos + 1, string.len(n))
                    if not uniques2[unique_index_name] then
                        uniques2[unique_index_name] = {}
                    end
                    table.insert(uniques2[unique_index_name], column)
                end 
            end

            if string.upper(n) == "INDEX" then
                indexs["index_" .. self._tableName .. "_" .. column] = {column}
            else
                if string.find(n, "index") == 1 then
                    local pos = string.find(n, ":")
                    local index_name = string.sub(n, pos + 1, string.len(n))
                    if not indexs[index_name] then
                        indexs[index_name] = {}
                    end
                    table.insert(indexs[index_name], column)
                end 
            end

            if string.find(n, "comment") == 1 then
                local pos3 = string.find(n, ":")
                comments[column] = string.sub(n, pos3 + 1, string.len(n))
            end
        end
    end

    local res = self._orm_db.db:query("desc " .. self._tableName)

    if res.errno == 1146 then
        local sql = "CREATE TABLE `" .. self._tableName .. "`("

        for _, v in pairs(self._columns) do

            local tags = string.split(v, ";")
            local column = ""
            for k, n in pairs(tags) do
                n = string.trim(n)
                if string.find(n, "column") == 1 then
                    local pos = string.find(n, ":")
                    column = string.sub(n, pos + 1, string.len(n))
                end
            end

            if column == "" then
                assert(nil, v ..  " tag err, not find 'column'")
            end

            sql = sql .. string.format("`%s` ", column)
            if not types[column] then types[column] = "int" end
            sql = sql .. types[column] .. " "
            
            if notNulls[column] then
                sql = sql .. "NOT NULL "
            end
 
            if defaults[column] then
                sql = sql .. "DEFAULT " .. defaults[column] .. " "
            end
            if uniques[column] then
                sql = sql .. "UNIQUE "
            end
            if auto_increments[column] then
                sql = sql .. "AUTO_INCREMENT "
            end

            if comments[column] then
                sql = sql .. "COMMENT " .. comments[column] .. " "
            end
            sql = sql .. ", "
        end
        sql = string.sub(sql, 1, string.len(sql) - 2)

        if #primaryKeys == 0 then
            table.insert(primaryKeys, "id")
        end

        sql = sql .. ", PRIMARY KEY(" 
        for _, column in pairs(primaryKeys) do
            sql = sql .. "`" .. column .. "`,"
        end
        sql = string.sub(sql, 1, string.len(sql) - 1) .. "))"

        self._orm_db:assert_query(sql)

        for index_name, columns in pairs(indexs) do
            local indexSql = string.format("CREATE INDEX `%s` ON `%s`(", index_name, self._tableName)
            for _, column in pairs(columns) do
                indexSql = indexSql .. "`" .. column .."`,"
            end
            indexSql = string.sub(indexSql, 1, string.len(indexSql) - 1) .. ")"
            self._orm_db:assert_query(indexSql)
        end

        for index_name, columns in pairs(uniques2) do
            local indexSql = string.format("CREATE UNIQUE INDEX `%s` ON `%s`(", index_name, self._tableName)
            for _, column in pairs(columns) do
                indexSql = indexSql .. "`" .. column .."`,"
            end
            indexSql = string.sub(indexSql, 1, string.len(indexSql) - 1) .. ")"
            self._orm_db:assert_query(indexSql)
        end
    else
        local alterColums = {}
        for _, v in pairs(self._columns) do

            local tags = string.split(v, ";")
            local column = ""
            for k, n in pairs(tags) do
                n = string.trim(n)
                if string.find(n, "column") == 1 then
                    local pos = string.find(n, ":")
                    column = string.sub(n, pos + 1, string.len(n))
                end
            end

            if column == "" then
                assert(nil, v ..  " tag err, not find 'column'")
            end

            local find = false
            for k, v2 in pairs(res) do
                if column == v2.Field then find = true end
            end
            if not find then
                table.insert(alterColums, column)
            end
        end

        if #alterColums > 0 then

            local sql = "ALTER TABLE `" .. self._tableName .. "` "

            for k, column in pairs(alterColums) do

                sql = sql .. string.format("ADD COLUMN `%s` ", column)
                if not types[column] then types[column] = "int" end
                sql = sql .. types[column] .. " "

                if notNulls[column] then
                    sql = sql .. "NOT NULL "
                end
                if defaults[column] then
                    sql = sql .. "DEFAULT " .. defaults[column] .. " "
                end
                if uniques[column] then
                    sql = sql .. "UNIQUE "
                end
                if auto_increments[column] then
                    sql = sql .. "AUTO_INCREMENT "
                end
                if comments[column] then
                    sql = sql .. "COMMENT " .. comments[column] .. " "
                end
                sql = sql .. ", "
            end

            sql = string.sub(sql, 1, string.len(sql) - 2)
            self._orm_db:assert_query(sql)

            local indexRows = self._orm_db:assert_query("SHOW INDEX FROM `" .. self._tableName .. "`")

            for index_name, columns in pairs(indexs) do
                local find = false
                for k, v in pairs(indexRows) do
                    if v.Key_name == index_name then find = true end
                end
                if not find then
                    local indexSql = string.format("CREATE INDEX `%s` ON `%s`(", index_name, self._tableName)
                    for _, column in pairs(columns) do
                        indexSql = indexSql .. "`" .. column .."`,"
                    end
                    indexSql = string.sub(indexSql, 1, string.len(indexSql) - 1) .. ")"
                    self._orm_db:assert_query(indexSql)
                end
            end

            for index_name, columns in pairs(uniques2) do
                local find = false
                for k, v in pairs(indexRows) do
                    if v.Key_name == index_name then find = true end
                end
                if not find then
                    local indexSql = string.format("CREATE UNIQUE INDEX `%s` ON `%s`(", index_name, self._tableName)
                    for _, column in pairs(columns) do
                        indexSql = indexSql .. "`" .. column .."`,"
                    end
                    indexSql = string.sub(indexSql, 1, string.len(indexSql) - 1) .. ")"
                    self._orm_db:assert_query(indexSql)
                end
            end
        end
    end
end

return ORM