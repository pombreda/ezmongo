from test_schema import Field, Schema

myschema = Schema("mytable")
myschema.add_fields(["user", "range", "metric", "ccypair"])
myschema.add_fields({"date": int, "tradedate": int})

val_schema = Schema()
val_schema.add_fields(["orders", "trades"], float)

myschema.add_array("vals", object, val_schema, always_unwind=True)

#print myschema
# Representation of a schema similar to the one depicted below:
#
# {
#     user: str,
#     range: str,
#     metric: str,
#     vals: [
#         {
#             orders: int,
#             trades: int
#         }
#     ]
# }