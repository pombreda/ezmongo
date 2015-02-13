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
    print "----------execute_mongo---------------"
    print match, unwind, project, group, sort, limit
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
    print pipeline
    res = coll.aggregate(pipeline)
    for r in res.get("result"):
        print r


class SqlToMongo():
    def __init__(self, schema):
        self.dbg = 3
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
        print "Gen_sel", sel
        fldtype = sel.get("type")
        if fldtype == "id":
            # For something like "ORDERS as CLIENTORDERS" project the alias
            alias = sel.get("alias", fld)
        elif fldtype == "func":
            # for something like "SUM(ORDERS) as TOTALORDERS" don't project the alias as GROUP BY will take care
            # of it after aggregation
            alias = fld
        if not self.schema.is_array(fld):
            return {"{FLD}".format(FLD=alias): "${FLD}".format(FLD=fld)}, "id"
        else:
            return {
                "{FLD}".format(FLD=alias): "${FLD}".format(FLD=fld)
            }, "array"

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


    def gen_grp(groupby, sel_funcs):
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
        unwinds = []
        project = None
        if len(select):
            project = {"$project": {}}
        for fld, sel in select.items():
            print fld
            if sel.get("type") == "func":
                sel_funcs.append(sel)
            res, restype = self.gen_sel(fld, sel)
            project.get("$project").update(res)
            if restype == "array" and self.schema.get_always_unwind(fld):
                unwinds.append(fld)

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
        if match:
            print match
        if project:
            print project
        unwind = None
        if unwinds:
            print unwinds
            unwind = [{"$unwind": "${FLD}".format(FLD=fld)} for fld in unwinds]
        if group:
            print group
        if sort:
            print sort
        if limit:
            print limit
        # execute

        execute_mongo(match, unwind, project, group, sort, limit)


    def parse_function(self, tok):
        print "\t\tparse_function:tok=%s,tok.tokens=%s"%(tok, tok.tokens)
        oper, ident = None, None
        for subtok in tok.tokens:
            # print subtok, type(subtok)
            if isinstance(subtok, sqlparse.sql.Parenthesis):
                # print "Got paran", subtok.tokens
                for parentok in subtok.tokens:
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
        last_tok = None
        curr_fld = None
        for tok in res[0].tokens:
            if tok.is_whitespace():
                last_tok = tok
                continue
            tokval = tok.value.strip().lower()
            if self.dbg >= 3:
                print "-----*%s*"%tokval, type(tok), tok.is_keyword, state, "-----"
            if tokval in puncts:
                last_tok = tok
                continue
            if tok.is_keyword:
                # change state if know keyword otherwise leave state unchanged
                if tokval in ("from", "order", "limit", "group", "select"):
                    state = tokval.upper()
                    if self.dbg >= 3:
                        print "state -->", state
                    last_tok = tok
                    continue
                if tokval in ("by"):
                    last_tok = tok
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
                            else:
                                print "\t\tADDING SELECT 3:", {"fld": idval, "type": "id"}
                                select[idval] = {'fld': idval, 'type': 'id'}
            elif isinstance(tok, sqlparse.sql.Function):
                print "Got a function", tok.tokens
                funcparts = self.parse_function(tok)
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
        "select userid, vals.min as valmin from sidtest where metric='latency' order by orders desc limit 3")

    # todo: above query doesn't work (alias with projecting array)
    # todo: select userid, vals.min from sidtest where metric='latency' order by orders desc limit 3 --> this
        # todo: query projects the correct element 'min' of the array but ends up making an object with an element min
        # todo: in the output as well

    #sqm.run_mongo(
    #    "select user, sum(vals.orders) from sidtest where tradedate=20150205 and ccypair='EURUSD' and metric='qd' order by orders desc limit 3")


    # convert_to_mongo(
    # "select date, min, sum(orders) as totord from sidtest where tradedate=20150205 and ccypair='EURUSD' and metric='qd' group by date, min order by totord desc limit 3")




    # match = {'$match': {'metric': 'qd', 'ccypair': 'EURUSD', 'tradedate': 20150205}}
    # project = {'$project': {'date': '$date', 'trades': '$vals.trades', 'orders': '$vals.orders', 'minute': '$vals.minute'}}
    # sort = {'$sort': {'orders': -1}}
    # limit = {'$limit': 3}

    # query with sum

    # ORIGINAL

    # match = {'$match': {'metric': 'qd', 'ccypair': 'EURUSD', 'tradedate': 20150205}}
    # unwind
    # project = {'$project': {'date': '$date', 'min': '$vals.min', 'orders': '$vals.orders'}}
    # group = {$group: {_id: {date: '$date', minute: '$minute'}, totord: {$sum: "orders"}}}
    # sort = {'$sort': {'totord': -1}}
    # limit = {'$limit': 3}


    #GENERATED

    #match = {'$match': {'metric': 'qd', 'ccypair': 'EURUSD', 'tradedate': 20150205}}
    #project = {'$project': {'date': '$date', 'orders': '$vals.orders', 'min': '$vals.min'}}
    #group = {'$group': {'_id': {'date': '$date', 'min': '$min'}, 'totord': {'$sum': 'orders'}}}

