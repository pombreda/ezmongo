__author__ = 'sidazad'


class Field(object):
    def __init__(self, name="", fldtype=str, content_type=None, schema=None, always_unwind=False):
        """
            :param always_unwind: Arrays may or may not be unwound in $project if there is no aggregate operator
            like $sum or $avg involved. If always_unwind is True then the array will always be unwound, hence
            resulting in multiple rows for each element of the array (as opposed to a single row with an array).
            In SQL terms, with always_unwind=True the data will be normalized.
        """
        self.name = name
        self.type = fldtype
        self.content_type = content_type
        self.schema = schema
        self.always_unwind = always_unwind

    def is_array(self):
        return self.fldtype == "array"

class Schema(object):
    """
        Description of a MongoDB table.
    """
    def __init__(self, table=""):
        self.table = table
        self.fields = []
        self.array_fields = []

    def add_array(self, name, content_type=str, schema=None, always_unwind=False):
        """
            :param name: Name of the array field
            :param content_type: type of the elements of the array field (str|int|object etc.)
            :param schema: Object of class Schema - used if the elements of the array are objects themselves
            If the element of this array is going to be another object
        """
        self.array_fields.append(Field(name, "array", content_type, schema, always_unwind=always_unwind))

    def add_fields(self, flds, fldtype=str):
        if isinstance(flds, list):
            for fld in flds:
                self.fields.append(Field(fld, fldtype))
        else:
            for fld, tmpfldtype in flds.items():
                self.fields.append(Field(fld, tmpfldtype))

    def is_array(self, fldname):
        for fld in self.array_fields:
            if fld.name == fldname:
                return True
        return False

    def get_fld_val(self, fldname, fldval):
        if fldname in self.fields:
            return self.fields[self.fields.index(fldname)].type(fldval)

    def __str__(self):
        return """
        Fields: %s
        Arrays: %s
        """ % ([fld.name for fld in self.fields], [[fld.name, fld.type, fld.content_type, str(fld.schema)] for fld in self.array_fields])



if __name__ == "__main__":
    d = execfile("tmpconf.py")
    print d