import msgpack, numpy, io

a_filenames=['/tmp/595f6d66-6fd2-49af-9237-a51b53240f50-a-0.json'] 
b_filenames=['/tmp/595f6d66-6fd2-49af-9237-a51b53240f50-b-0.json']

cdef class Merge:
    cdef int numfiles[2]
    cdef int file_idx[2]
    cdef object[:] file
    cdef object[:] unpacker
    cdef list[:] filenames
    cdef object[:] items
    cdef int more[2]

    cdef void next_file(Merge self, int idx):
        if self.file_idx[idx] > 0:
            self.file[idx].close()
        if self.file_idx[idx] >= self.numfiles[idx]:
            self.file_idx[idx] = -1
            return
        self.file[idx] = open(self.filenames[idx][self.file_idx[idx]], 'rb')
        self.unpacker[idx] = msgpack.Unpacker(self.file[idx], raw=False)
        self.file_idx[idx] += 1

    cdef void next_item(Merge self, int idx):
        while True:
            if self.file_idx[idx] == -1:
                self.more[idx] = 0
                return
            try:
                self.items[idx] = next(self.unpacker[idx])
            except StopIteration:
                self.next_file(idx)

    def merge(Merge self, list filenames, out_filenames):
        cdef int idx

        self.out_filenames = out_filenames
        self.filenames = numpy.array(filenames,dtype=object)
        self.file = numpy.array([None, None],dtype=object)
        self.unpacker = numpy.array([None, None],dtype=object)
        self.items = numpy.array([None, None],dtype=object)
        for idx in range(0, 2):
            self.file_idx[idx] = 0
            self.numfiles[idx] = len(self.filenames[idx])
            self.more[idx] = 1
            self.next_file(idx)
            self.next_item(idx)
            
        while self.more[0] and self.more[1]:
            if self.items[0] < self.items[1]:
                yield self.items[0]
                self.next_item(0)
            else:
                yield self.items[1]
                self.next_item(1)
        while self.more[0]:
            yield self.items[0]
            self.next_item(0)
        while self.more[1]:
            yield self.items[1]
            self.next_item(1)

merge = Merge()
list(merge.merge([a_filenames, b_filenames]))
    
