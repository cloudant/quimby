

SIMPLE_DOCS = [
    {"_id": "Rob", "location": "Bristol"},
    {"_id": "Ulises", "location": "Aberdeen"},
    {"_id": "Simon", "location": "Bristol"},
    {"_id": "Bob", "location": "Windsor"},
    {"_id": "Mike", "location": "Bristol"}
]


SIMPLE_MAP_RED_DDOC = {
    "_id": "_design/foo",
    "views": {
        "bar": {
            "map": "function(doc) {emit(doc.key, doc.val);}"
        },
        "bam": {
            "map": "function(doc) {emit(doc.int % 2, doc.val);}",
            "reduce": "_sum"
        }
    }
}


def gen_docs(count=25, value=0):
    ret = []
    for i in range(count):
        ret.append({
            "_id": "%06d" % i,
            "int": i,
            "val": value
        })
    return ret
