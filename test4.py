import msgpack

a_filenames=['/tmp/595f6d66-6fd2-49af-9237-a51b53240f50-a-0.json'] 
b_filenames=['/tmp/595f6d66-6fd2-49af-9237-a51b53240f50-b-0.json']

def fileiter(filenames):
    for filename in filenames:
        with open(filename, 'rb') as f:
            unpacker = msgpack.Unpacker(f, raw=False)
            for msg in unpacker:
                yield msg

def merge(a, b):
    a = iter(a)
    b = iter(b)
    bpeek = None
    for aval in a:
        if bpeek is not None:
            if bpeek < aval:
                yield bpeek
                bpeek = None
            else:
                break
        else:
            for bval in b:
                if bval < aval:
                    yield bval
                else:
                    bpeek = bval
                    break
        yield aval
    for bval in b:
        yield bval

x = list(merge(fileiter(a_filenames), fileiter(b_filenames)))
