__author__ = 'sidazad'

import pymongo
import sqlparse
from pymongo import MongoClient
import datetime
import timeit

puncts = [","]
known_funcs = ['sum', 'count']

opmaps = {
    ">": "$gt",
    "<": "$gt",
    ">=": "$gte",
    "<=": "$lte",
    "<>": "$ne",
}


def execute_mongo(match, unwind, project, group, sort, limit):
    dbg = 0
    #print "----------execute_mongo---------------"
    #print match, unwind, project, group, sort, limit
    client = MongoClient('localhost', 27017)
    db = client.sidtest
    coll = db.sidtest
    pipeline = [match]
    if unwind:
        for ele in unwind:
            pipeline.append(ele)
    if project:
        pipeline.append(project)
    if group:
        pipeline.append(group)
    if sort:
        pipeline.append(sort)
    if limit:
        pipeline.append(limit)
    if dbg > 0:
        print pipeline
    res = coll.aggregate(pipeline)
    return res


class SqlToMongo():
    def __init__(self, schema):
        self.dbg = 0
        self.schema = schema

    def gen_cond(self, cond):
        op = cond.get('op')
        val = cond.get('val')
        fld = cond.get('fld')

        if val.startswith("'"):
            val = val[1:-1]
        else:
            val = int(val)
        if op == "=":
            return {cond.get("fld"): val}
        elif op in opmaps.keys():
            return {cond.get("fld"): {opmaps.get("fld"): val}}
        return None

    def gen_sel(self, fld, sel):
        if self.dbg > 3:
            print "Gen_sel", sel
        fldtype = sel.get("type")
        if fldtype == "id":
            # For something like "ORDERS as CLIENTORDERS" project the alias
            alias = sel.get("alias", fld)
        elif fldtype == "func":
            # for something like "SUM(ORDERS) as TOTALORDERS" don't project the alias as GROUP BY will take care
            # of it after aggregation
            alias = fld
        if sel.get("is_array"):
            if sel.get("array_fld", None):
                # if array of objects then project specific field
                return {"{FLD}".format(FLD=alias): "${FLD}".format(FLD=fld)}
            else:
                # if not an array of objects then project whole array
                return {"{FLD}".format(FLD=alias): "${FLD}".format(FLD=sel.get("array_name"))}
        else:
            return {
                "{FLD}".format(FLD=alias): "${FLD}".format(FLD=fld)
            }

        return None


    def gen_sort(self, fld, srt):
        """
            fld is expected to be of the format:
                [<fldname> asc|<fldname> desc|<fldname>]
        """
        fldtoks = fld.strip().split()
        sortfld, order = fld, 1
        if len(fldtoks) == 2:
            sortfld = fldtoks[0]
            if fldtoks[1] == "desc":
                order = -1
        return {sortfld: order}


    def gen_grp(self, groupby, sel_funcs):
        group = {"$group": {}}
        inner = group.get("$group")
        if not len(groupby):
            inner.update({"_id": "null"})
        else:
            inner.update({"_id": {}})
            id_dict = inner.get("_id")
            for fld, grp in groupby.items():
                id_dict.update({grp.get("alias", grp.get("fld")): "${FLD}".format(FLD=grp.get("fld"))})
                # group = {$group: {_id: {date: '$date', minute: '$minute'}, totord: {$sum: "orders"}}
        for func in sel_funcs:
            inner.update({func.get("alias", func.get("fld")): {
                "${OP}".format(OP=func.get("op")): "${FLD}".format(FLD=func.get("fld"))}})
        return group


    def gen_mongo(self, select, table, conditions, groupby, order, limit):
        if self.dbg > 1:
            print "-------------------gen_mongo-------------------"
            print "SELECT=", select
            print "table=", table
            print "conditions=", conditions
            print "group=", groupby
            print "order=", order
            print "limit=", limit

        match = None
        if len(conditions):
            match = {"$match": {}}

        for fld, cond in conditions.items():
            res = self.gen_cond(cond)
            if res:
                match.get("$match").update(res)

        sel_funcs = []
        sel_data = []
        unwinds = []
        project = None
        if len(select):
            project = {"$project": {}}
        has_array, has_func = False, False
        for fld, sel in select.items():
            dottoks = fld.split(".")
            is_array = False
            if self.schema.is_array(dottoks[0]) or (len(dottoks) == 2 and self.schema.is_array(dottoks[0])):
                is_array = True
                if not has_array:
                    has_array = True
            sel["is_array"] = is_array
            if is_array:
                sel["array_name"] = dottoks[0]
                if len(dottoks) == 2:
                    # array of objects (e.g. vals.min) as opposed to array of basic types
                    sel["array_fld"] = dottoks[1]
                    unwinds.append(sel)
            if sel.get("type") == "func":
                sel_funcs.append(sel)
                if not has_func:
                    has_func = True
            res = self.gen_sel(fld, sel)
            project.get("$project").update(res)



        group = None
        if len(groupby) or len(sel_funcs):
            # if there are any explicit group by or an aggregate functions in the select list
            group = {"$group": {}}
            group = self.gen_grp(groupby, sel_funcs)


        # generate sort
        sort = None
        if len(order):
            sort = {"$sort": {}}
        for fld, srt in order.items():
            res = self.gen_sort(fld, srt)
            sort.get("$sort").update(res)
        if limit != -1:
            limit = {"$limit": limit}
        unwind = []
        unwound = []
        if unwinds:
            for fld in unwinds:
                array_name = fld.get("array_name")
                if not array_name in unwound:
                    unwind.append({"$unwind": "${FLD}".format(FLD=array_name)})
                    unwound.append(array_name)

        # execute

        return execute_mongo(match, unwind, project, group, sort, limit)


    def parse_function(self, tok):
        print "\t\tparse_function:tok=%s,tok.tokens=%s"%(tok, tok.tokens)
        oper, ident = None, None
        for subtok in tok.tokens:
            if isinstance(subtok, sqlparse.sql.Parenthesis):
                for parentok in subtok.tokens:
                    if self.dbg > 3:
                        print "\t\tparentok=%s"%parentok,type(parentok)
                    if isinstance(parentok, sqlparse.sql.Identifier):
                        ident = parentok.value.strip()
            elif isinstance(subtok, sqlparse.sql.Identifier):
                val = subtok.value.strip().lower()
                if val in known_funcs:
                    oper = val
        return {'type': 'func', 'op': oper, 'fld': ident}


    def convert_to_mongo(self, sqlstr):
        state, sub_state = "SELECT", None
        conditions, select, group, order = {}, {}, {}, {}
        limit = -1
        table = ""
        res = sqlparse.parse(sqlstr)
        if self.dbg >= 3:
            print "-----", res, type(res[0]), "-----"
        for tok in res[0].tokens:
            if tok.is_whitespace():
                continue
            tokval = tok.value.strip().lower()
            if self.dbg >= 3:
                print "-----*%s*"%tokval, type(tok), tok.is_keyword, state, "-----"
            if tokval in puncts:
                continue
            if tok.is_keyword:
                # change state if know keyword otherwise leave state unchanged
                if tokval in ("from", "order", "limit", "group", "select"):
                    state = tokval.upper()
                    if self.dbg >= 3:
                        print "state -->", state
                    continue
                if tokval in ("by"):
                    continue
            if isinstance(tok, sqlparse.sql.Identifier):
                if state == "SELECT":
                    select[tokval] = {"fld": tokval, "type": "id"}
                elif state == "FROM":
                    table = tokval
                elif state == "ORDER":
                    order[tokval] = {'fld': tokval, 'type': 'id'}
            elif isinstance(tok, sqlparse.sql.IdentifierList):
                for id in tok.get_identifiers():
                    idval = id.value.strip().lower()
                    if self.dbg >= 3:
                        print "\t\tidlist:id=", id, type(id), dir(id)
                    if isinstance(id, sqlparse.sql.Function):
                        funcparts = self.parse_function(id)
                        if self.dbg >= 3:
                            print "\t\tFUNCTION-X:", funcparts
                            if state == "SELECT":
                                select[funcparts.get("fld")] = funcparts
                    elif isinstance(id, sqlparse.sql.Token) or isinstance(id, sqlparse.sql.Identifier):
                        if state == "SELECT":
                            if isinstance(id, sqlparse.sql.Identifier) and id.is_group() and id.get_alias():
                                # aliases such as <field as alias> or <SUM(field) as alias> as they come as groups
                                # with a top level IDENTIFIER (as opposed to TOKEN when there is no alias)
                                # The first member of the group is a Token instance if it's a regular field
                                # and a Function if it is a function such as SUM(field)
                                alias = id.get_alias().strip().lower()
                                orig = id.token_first()
                                if isinstance(orig, sqlparse.sql.Function):
                                    funcparts = self.parse_function(orig)
                                    funcparts["alias"] = alias
                                    if self.dbg >= 3:
                                        print "\t\tADDING SELECT 2:", funcparts
                                    select[funcparts.get("fld")] = funcparts
                                elif isinstance(id, sqlparse.sql.Identifier):
                                    idstr = ""
                                    for tmptok in id.tokens:
                                        tmptoktxt = tmptok.value.strip().lower()
                                        if tmptoktxt == "as":
                                            break
                                        idstr += tmptoktxt
                                    select[idstr] = {'fld': idstr, 'type':'id', 'alias': alias}
                            else:
                                if self.dbg > 3:
                                    print "\t\tADDING SELECT 3:", {"fld": idval, "type": "id"}
                                select[idval] = {'fld': idval, 'type': 'id'}
            elif isinstance(tok, sqlparse.sql.Function):
                if self.dbg > 3:
                    print "Got a function", tok.tokens
                funcparts = self.parse_function(tok)
                if self.dbg > 3:
                    print "ADDING SELECT 4:", funcparts
                select[funcparts.get("fld")] = funcparts
                if self.dbg >= 3:
                    print "FUNCTION-Y:", funcparts
            elif isinstance(tok, sqlparse.sql.Where):
                if self.dbg >= 3:
                    print "WHERE CLAUSE"
                for wheretok in tok.tokens:
                    if self.dbg >= 3:
                        print wheretok, type(wheretok)
                    if isinstance(wheretok, sqlparse.sql.Comparison):
                        if self.dbg >= 3:
                            print "COMP:", wheretok.tokens
                        matchparams = {}
                        for moretoks in wheretok.tokens:
                            if self.dbg >= 3:
                                print moretoks, type(moretoks)
                            if isinstance(moretoks, sqlparse.sql.Identifier):
                                matchparams["fld"] = moretoks.value.strip().lower()
                            elif isinstance(moretoks, sqlparse.sql.Token):
                                tmpval = moretoks.value.strip().lower()
                                if tmpval in ["=", "<", ">", ">=", "<=", "<>"]:
                                    matchparams["op"] = tmpval
                                else:
                                    matchparams["val"] = moretoks.value.strip()
                        conditions[matchparams.get("fld")] = matchparams
            elif isinstance(tok, sqlparse.sql.Token):
                if state == "SELECT":
                    print "ADDING SELECT 5:", {"fld": tokval, "type": "id"}
                    select[tokval] = {"fld": tokval, "type": "id"}
                elif state == "LIMIT":
                    limit = int(tokval)
                elif state == "GROUP":
                    group[tokval] = {'fld': tokval, 'type': 'id'}
            last_tok = tok
        return select, table, conditions, group, order, limit

    def run_mongo(self, sqlstr):
        conditions, select, group, order = {}, {}, {}, {}
        table, limit = "", -1
        select, table, conditions, group, order, limit = self.convert_to_mongo(sqlstr)
        self.gen_mongo(select, table, conditions, group, order, limit)


# https://sqlparse.readthedocs.org/en/latest/analyzing/#analyze

def mockdata():
    """
        Total 20 documents
        10 for collat "abc" out of which 5 with ccypair "EURUSD" and 5 with "ALL"
        10 for collat "def" out of which 5 with ccypair "EURUSD" and 5 with "ALL"
        Each document has vals array with mins 0 to 59 with each min having orders = 3*minute and trades = 2*minute
        (for example min: 5 has orders: 15 and trades :10)
    """
    client = MongoClient('localhost', 27017)
    db = client.sidtest
    coll = db.sidtest
    coll.remove()
    doc = {"ccypair": "ALL", "collat": "abc", "date": datetime.datetime.utcnow(), "tradedate": 20150205, "metric": "qd"}
    doc["vals"] = [{'min': i, 'orders': 3 * i, 'trades': 2 * i} for i in range(60)]
    docs = []
    for i in range(5):
        tmpdoc = doc.copy()
        docs.append(tmpdoc)
    for i in range(5):
        tmpdoc = doc.copy()
        tmpdoc["collat"] = "def"
        docs.append(tmpdoc)
    for i in range(5):
        tmpdoc = doc.copy()
        tmpdoc["ccypair"] = "EURUSD"
        docs.append(tmpdoc)
    for i in range(5):
        tmpdoc = doc.copy()
        tmpdoc["ccypair"] = "EURUSD"
        tmpdoc["collat"] = "def"
        docs.append(tmpdoc)
    coll.insert(docs)


if __name__ == "__main__":
    # mockdata()
    schema = {}
    execfile("tmpconf.py", schema)
    print schema
    sqm = SqlToMongo(schema.get("myschema"))
    sqm.run_mongo(
        #"select userid, sum(vals.min) as valmin from sidtest where metric='latency' order by orders desc limit 3")
        #"select userid, vals as valmin from sidtest where metric='latency' order by orders desc limit 3")
        "select userid, vals.min as valmin from sidtest where metric='latency' order by orders desc limit 3")

    # todo: write test cases for the 3 (2 commented and 1 live) queries above - believe they are working correctly
    # todo: now.
    # todo: Test grouping (group by)



