__author__ = 'sidazad'


import pymongo
import sqlparse

known_funcs = ['SUM', 'COUNT']

def gen_mongo(match):
    print "gen_mongo"

def parse_function(tok):
    oper, ident = None, None
    for subtok in tok.tokens:
        #print subtok, type(subtok)
        if isinstance(subtok, sqlparse.sql.Parenthesis):
            #print "Got paran", subtok.tokens
            for parentok in subtok.tokens:
                if isinstance(parentok, sqlparse.sql.Identifier):
                    ident = parentok.value.strip()
        elif isinstance(subtok, sqlparse.sql.Identifier):
            val = subtok.value.strip().upper()
            if val in known_funcs:
                oper = val
    return (oper, ident)



def convert_to_mongo(sqlstr):
    match = []
    res = sqlparse.parse(sqlstr)
    print res, type(res[0])
    for tok in res[0].tokens:
        print tok, type(tok)
        if isinstance(tok, sqlparse.sql.IdentifierList):
            print "ID List"
            for id in tok.get_identifiers():
                print id, type(id)
                if isinstance(id, sqlparse.sql.Function):
                    print "Got a function"
                    funcparts = parse_function(id)
                    print funcparts
        elif isinstance(tok, sqlparse.sql.Function):
            print "Got a function",tok.tokens
            funcparts = parse_function(tok)
            print funcparts
        elif isinstance(tok, sqlparse.sql.Identifier):
            print "Got single ID"
            print tok, type(tok)
        elif isinstance(tok, sqlparse.sql.Where):
            print "WHERE CLAUSE"
            for wheretok in tok.tokens:
                print wheretok, type(wheretok)
                if isinstance(wheretok, sqlparse.sql.Comparison):
                    print "COMP:",wheretok.tokens
                    matchparams = {}
                    for moretoks in wheretok.tokens:
                        print moretoks, type(moretoks)
                        if isinstance(moretoks, sqlparse.sql.Identifier):
                            matchparams["fld"] = moretoks.value
                        elif isinstance(moretoks, sqlparse.sql.Token):
                            tmpval = moretoks.value.strip()
                            if tmpval in ["=", "<", ">", ">=", "<=", "<>"]:
                                matchparams["op"] = tmpval
                            else:
                                matchparams["val"] = moretoks.value
                    match.append(matchparams)
    print match
    gen_mongo(match)






# https://sqlparse.readthedocs.org/en/latest/analyzing/#analyze

if __name__ == "__main__":
    convert_to_mongo("select sum(orders) from hsdata where tradedate=20150900 and ccypair='all' and collat='america'")
    if False:
        res = sqlparse.parse("select tradedate, sum(order) from orders where user in (select users from usertable where acctype in (1,2,3)) group by tradedate limit 30")
        print res, type(res[0])
        print res[0].tokens
        print res[0].to_unicode()
        for tok in res[0].tokens:
            print tok, type(tok)



