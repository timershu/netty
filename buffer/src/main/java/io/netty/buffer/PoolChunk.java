/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.netty.buffer;

import io.netty.util.collection.IntObjectHashMap;

import java.util.Arrays;

/**
 * Description of algorithm for PageRun/PoolSubpage allocation from PoolChunk
 *
 * Notation: The following terms are important to understand the code
 * > page  - a page is the smallest unit of memory chunk that can be allocated
 * > chunk - a chunk is a collection of pages
 * > in this code chunkSize = 2^{maxOrder} * pageSize
 *
 * To begin we allocate a byte array of size = chunkSize
 * Whenever a ByteBuf of given size needs to be created we search for the first position
 * in the byte array that has enough empty space to accommodate the requested size and
 * return a (long) handle that encodes this offset information, (this memory segment is then
 * marked as reserved so it is always used by exactly one ByteBuf and no more)
 *
 * For simplicity all sizes are normalized according to PoolArena#normalizeCapacity method
 * This ensures that when we request for memory segments of size >= pageSize the normalizedCapacity
 * equals the next nearest power of 2
 *
 * To search for the first offset in chunk that has at least requested size available we construct a
 * complete balanced binary tree and store it in an array (just like heaps) - memoryMap
 *
 * The tree looks like this (the size of each node being mentioned in the parenthesis)
 *
 * depth=0        1 node (chunkSize)
 * depth=1        2 nodes (chunkSize/2)
 * ..
 * ..
 * depth=d        2^d nodes (chunkSize/2^d)
 * ..
 * depth=maxOrder 2^maxOrder nodes (chunkSize/2^{maxOrder} = pageSize)
 *
 * depth=maxOrder is the last level and the leafs consist of pages
 *
 * With this tree available searching in chunkArray translates like this:
 * To allocate a memory segment of size chunkSize/2^k we search for the first node (from left) at height k
 * which is unused
 *
 * Algorithm:
 * ----------
 * Encode the tree in memoryMap with the notation
 *   memoryMap[id] = x => in the subtree rooted at id, the first node that is free to be allocated
 *   is at depth x (counted from depth=0) i.e., at depths [depth_of_id, x), there is no node that is free
 *
 *  As we allocate & free nodes, we update values stored in memoryMap so that the property is maintained
 *
 * Initialization -
 *   In the beginning we construct the memoryMap array by storing the depth of a node at each node
 *     i.e., memoryMap[id] = depth_of_id
 *
 * Observations:
 * -------------
 * 1) memoryMap[id] = depth_of_id  => it is free / unallocated
 * 2) memoryMap[id] > depth_of_id  => at least one of its child nodes is allocated, so we cannot allocate it, but
 *                                    some of its children can still be allocated based on their availability
 * 3) memoryMap[id] = maxOrder + 1 => the node is fully allocated & thus none of its children can be allocated, it
 *                                    is thus marked as unusable
 *
 * Algorithm: [allocateNode(d) => we want to find the first node (from left) at height h that can be allocated]
 * ----------
 * 1) start at root (i.e., depth = 0 or id = 1)
 * 2) if memoryMap[1] > d => cannot be allocated from this chunk
 * 3) if left node value <= h; we can allocate from left subtree so move to left and repeat until found
 * 4) else try in right subtree
 *
 * Algorithm: [allocateRun(size)]
 * ----------
 * 1) Compute d = log_2(chunkSize/size)
 * 2) Return allocateNode(d)
 *
 * Algorithm: [allocateNewSubpage(size)]
 * ----------
 * 1) use allocateRun(maxOrder) to find an empty (i.e., unused) leaf (i.e., page)
 * 2) use this handle to construct the poolsubpage object or if it already exists just initialize it
 *    with required normCapacity
 * 3) store (insert/ overwrite) the subpage in elemSubpages map for easier access
 *
 * Note:
 * -----
 * In the implementation for improving cache coherence,
 * we store 2 pieces of information (i.e, 2 byte vals) as a short value in memoryMap
 *
 * memoryMap[id]= (depth_of_id, x)
 * where as per convention defined above
 * the second value (i.e, x) indicates that the first node which is free to be allocated is at depth x (from root)
 */

final class PoolChunk<T> {

    private static final int BYTE_LENGTH = 8;
    private static final int LOWER_BYTE_MASK = 0xFF;
    private static final int UPPER_BYTE_MASK = ~LOWER_BYTE_MASK;

    private static final int INT_LENGTH = 32;
    private static final long LOWER_INT_MASK = 0xFFFFFFFFL;
    private static final long UPPER_INT_MASK = ~LOWER_INT_MASK;
    private static final long UNAVAILABLE = (LOWER_INT_MASK << INT_LENGTH) | LOWER_INT_MASK;
    private static final long NULLPTR = Integer.MAX_VALUE;
    private static final long SINGLE = (NULLPTR << INT_LENGTH) | NULLPTR;

    final PoolArena<T> arena;
    final T memory;
    final boolean unpooled;

    private final short[] memoryMap;
    private final PoolSubpage<T>[] subpages;
    private final long[] availLists;
    private final IntObjectHashMap<PoolSubpage<T>> elemSubpages;
    // TODO - replace with intlong primitive map when available to avoid creating Long objects unnecessarily
    private final IntObjectHashMap<Long> availListIndex;
    /** Used to determine if the requested capacity is equal to or greater than pageSize. */
    private final int subpageOverflowMask;
    private final int pageSize;
    private final int pageShifts;
    private final int maxOrder;
    private final int chunkSize;
    private final int log2ChunkSize;
    private final int maxSubpageAllocs;

    /** Used to mark memory as unusable */
    private final byte unusable;

    private int freeBytes;

    PoolChunkList<T> parent;
    PoolChunk<T> prev;
    PoolChunk<T> next;

    // TODO: Test if adding padding helps under contention
    //private long pad0, pad1, pad2, pad3, pad4, pad5, pad6, pad7;

    PoolChunk(PoolArena<T> arena, T memory, int pageSize, int maxOrder, int pageShifts, int chunkSize) {
        unpooled = false;
        this.arena = arena;
        this.memory = memory;
        this.pageSize = pageSize;
        this.pageShifts = pageShifts;
        this.maxOrder = maxOrder;
        this.chunkSize = chunkSize;
        unusable = (byte) (maxOrder + 1);
        log2ChunkSize = log2(chunkSize);
        subpageOverflowMask = ~(pageSize - 1);
        freeBytes = chunkSize;

        assert maxOrder < 30 : "maxOrder should be < 30, but is : " + maxOrder;
        maxSubpageAllocs = 1 << maxOrder;

        // Generate the memory map.
        memoryMap = new short[maxSubpageAllocs << 1];
        int memoryMapIndex = 1;
        for (int d = 0; d <= maxOrder; ++d) { // move down the tree one level at a time
            short dd = (short) ((d << BYTE_LENGTH) | d);
            for (int p = 0; p < (1 << d); ++p) {
                // in each level traverse left to right and set the depth of subtree
                // that is completely free to be my depth since I am totally free to start with
                memoryMap[memoryMapIndex] = dd;
                memoryMapIndex += 1;
            }
        }

        subpages = newSubpageArray(maxSubpageAllocs);
        availLists = newAvailList(maxSubpageAllocs);
        elemSubpages = new IntObjectHashMap<PoolSubpage<T>>(pageShifts + 512);
        availListIndex = new IntObjectHashMap<Long>(pageShifts + 512);
    }

    /** Creates a special chunk that is not pooled. */
    PoolChunk(PoolArena<T> arena, T memory, int size) {
        unpooled = true;
        this.arena = arena;
        this.memory = memory;
        memoryMap = null;
        subpages = null;
        elemSubpages = null;
        availLists = null;
        availListIndex = null;
        subpageOverflowMask = 0;
        pageSize = 0;
        pageShifts = 0;
        maxOrder = 0;
        unusable = (byte) (maxOrder + 1);
        chunkSize = size;
        log2ChunkSize = log2(chunkSize);
        maxSubpageAllocs = 0;
    }

    @SuppressWarnings("unchecked")
    private PoolSubpage<T>[] newSubpageArray(int size) {
        return new PoolSubpage[size];
    }

    private long[] newAvailList(int size) {
        long[] array = new long[size];
        Arrays.fill(array, UNAVAILABLE);
        return array;
    }

    int usage() {
        if (freeBytes == 0) {
            return 100;
        }

        int freePercentage = (int) (freeBytes * 100L / chunkSize);
        if (freePercentage == 0) {
            return 99;
        }
        return 100 - freePercentage;
    }

    long allocate(int normCapacity) {
        if ((normCapacity & subpageOverflowMask) != 0) { // >= pageSize
            return allocateRun(normCapacity);
        } else {
            return allocateSubpage(normCapacity);
        }
    }

    /**
     * Update method used by allocate
     * This is triggered only when a successor is allocated and all its predecessors
     * need to update their state
     * The minimal depth at which subtree rooted at id has some free space
     * @param id id
     */
    private void updateParentsAlloc(int id) {
        while (id > 1) {
            int parentId = id >>> 1;
            byte mem1 = value(id);
            byte mem2 = value(id ^ 1);
            byte mem = mem1 < mem2 ? mem1 : mem2;
            setVal(parentId, mem);
            id = parentId;
        }
    }

    /**
     * Update method used by free
     * This needs to handle the special case when both children are completely free
     * in which case parent be directly allocated on request of size = child-size * 2
     * @param id id
     */
    private void updateParentsFree(int id) {
        int logChild = depth(id) + 1;
        while (id > 1) {
            int parentId = id >>> 1;
            byte mem1 = value(id);
            byte mem2 = value(id ^ 1);
            byte mem = mem1 < mem2 ? mem1 : mem2;
            setVal(parentId, mem);
            logChild -= 1; // in first iteration equals log, subsequently reduce 1 from logChild as we traverse up

            if (mem1 == logChild && mem2 == logChild) {
                setVal(parentId, (byte) (logChild - 1));
            }

            id = parentId;
        }
    }

    private int allocateNode(int d) {
        int id = 1;
        byte mem = value(id);
        if (mem > d) { // unusable
            return -1;
        }
        while (mem < d || (id & (1 << d)) == 0) {
            id = id << 1;
            mem = value(id);
            if (mem > d) {
                id = id ^ 1;
                mem = value(id);
            }
        }
        setVal(id, unusable); // mark as unusable
        updateParentsAlloc(id);
        return id;
    }

    private long allocateRun(int normCapacity) {
        int numPages = normCapacity >>> pageShifts;
        int d = maxOrder - log2(numPages);
        int id = allocateNode(d);
        if (id < 0) {
            return id;
        }
        freeBytes -= runLength(id);
        return id;
    }

    private long allocateSubpage(int normCapacity) {
        PoolSubpage<T> subpage = elemSubpages.get(normCapacity);
        if (subpage != null) {
            long handle = subpage.allocate();
            if (handle > 0) {
                return handle;
            }
        }
        if (availListIndex.containsKey(normCapacity)) {
            if (availListIndex.get(normCapacity) != UNAVAILABLE) {
                return allocateAvailSubpage(normCapacity);
            }
        } else {
            availListIndex.put(normCapacity, UNAVAILABLE);
        }
        return allocateNewSubpage(normCapacity);
    }

    private long allocateAvailSubpage(int normCapacity) {
        long index = availListIndex.get(normCapacity);
        int head = (int) upper(index);

        assert head != LOWER_INT_MASK;
        PoolSubpage<T> subpage = subpages[head];
        assert normCapacity == subpage.elemSize;
        long handle = subpage.allocate();

        int pos = head;
        while (handle <= 0) {
            pos = removeHead(normCapacity, pos);

            if (pos == NULLPTR) {
                break;
            }
            subpage = subpages[pos];
            assert normCapacity == subpage.elemSize;
            handle = subpage.allocate();
        }
        if (handle > 0) {
            removeHead(normCapacity, pos);
            elemSubpages.put(normCapacity, subpage);
            return handle;
        }
        return allocateNewSubpage(normCapacity);
    }

    private long allocateNewSubpage(int normCapacity) {
        int d = maxOrder; // subpages are only be allocated from pages i.e., leaves
        int id = allocateNode(d);
        if (id < 0) {
            return id;
        }
        freeBytes -= pageSize;

        int subpageIdx = subpageIdx(id);
        PoolSubpage<T> subpage = subpages[subpageIdx];
        if (subpage == null) {
            subpage = new PoolSubpage<T>(this, id, runOffset(id), pageSize, normCapacity);
            subpages[subpageIdx] = subpage;
        } else {
            subpage.init(normCapacity);
        }
        elemSubpages.put(normCapacity, subpage); // store subpage at proper elemSize
        return subpage.allocate();
    }

    void free(long handle) {
        int memoryMapIdx = (int) lower(handle);
        int bitmapIdx = (int) upper(handle);
        int subpageId = subpageIdx(memoryMapIdx);
        if (bitmapIdx != 0) { // free a subpage
            PoolSubpage<T> subpage = subpages[subpageId];
            assert subpage != null && subpage.doNotDestroy;
            int normCapacity = subpage.elemSize;
            if (subpage.free(bitmapIdx & 0x3FFFFFFF)) {
                if (availLists[subpageId] == UNAVAILABLE) { // if not in availLists add to it
                    addTail(normCapacity, subpageId);
                }
                return;
            }
            if (availLists[subpageId] != UNAVAILABLE) { // if present in availLists remove from it
                removeElement(normCapacity, subpageId);
            }
            if (elemSubpages.get(normCapacity) == subpage) { // if present in elemSubpages remove from it
              elemSubpages.put(normCapacity, null);
            }
        }
        freeBytes += runLength(memoryMapIdx);
        setVal(memoryMapIdx, depth(memoryMapIdx));
        updateParentsFree(memoryMapIdx);
    }

    void initBuf(PooledByteBuf<T> buf, long handle, int reqCapacity) {
        int memoryMapIdx = (int) lower(handle);
        int bitmapIdx = (int) upper(handle);
        if (bitmapIdx == 0) {
            byte val = value(memoryMapIdx);
            assert val == unusable : String.valueOf(val);
            buf.init(this, handle, runOffset(memoryMapIdx), reqCapacity, runLength(memoryMapIdx));
        } else {
            initBufWithSubpage(buf, handle, bitmapIdx, reqCapacity);
        }
    }

    void initBufWithSubpage(PooledByteBuf<T> buf, long handle, int reqCapacity) {
        initBufWithSubpage(buf, handle, (int) upper(handle), reqCapacity);
    }

    private void initBufWithSubpage(PooledByteBuf<T> buf, long handle, int bitmapIdx, int reqCapacity) {
        assert bitmapIdx != 0;

        int memoryMapIdx = (int) handle;

        PoolSubpage<T> subpage = subpages[subpageIdx(memoryMapIdx)];
        assert subpage.doNotDestroy;
        assert reqCapacity <= subpage.elemSize;

        buf.init(
            this, handle,
            runOffset(memoryMapIdx) + (bitmapIdx & 0x3FFFFFFF) * subpage.elemSize, reqCapacity, subpage.elemSize);
    }

    private void addTail(int normCapacity, int pos) { // add node to tail of availLists
        long index = availListIndex.get(normCapacity);
        if (index == UNAVAILABLE) { // first node to be added so head = tail
            availLists[pos] = SINGLE; // prev = null, next = null at head/tail
            availListIndex.put(normCapacity, setUpper(pos, pos));
        } else {
            int tail = (int) lower(index); // find tail
            availLists[tail] = setLower(availLists[tail], pos); // set next = node pos
            availLists[pos] = setUpper(NULLPTR, tail); // set prev = tail, next = null i.,e new tail
            availListIndex.put(normCapacity, setLower(index, pos)); // register new tail
        }
    }

    private int removeHead(int normCapacity, int pos) { // remove node from head of availLists
        long val = availLists[pos];
        assert isHead(val) : String.valueOf(val);

        int next = (int) lower(val);
        long index = availListIndex.get(normCapacity);
        if (val == SINGLE) { // if this is the last node to be removed, register in map
            availListIndex.put(normCapacity, UNAVAILABLE);
        } else { // update availListIndex head to next of cur head
            availLists[next] = setUpper(availLists[next], NULLPTR); // next.prev = null
            availListIndex.put(normCapacity, setUpper(index, next));
        }
        availLists[pos] = UNAVAILABLE; // mark pos as unavailable
        return next;
    }

    private int removeTail(int normCapacity, int pos) { // remove node from tail of availLists
        long val = availLists[pos];
        assert isTail(val) : String.valueOf(val);

        int prev = (int) upper(val);
        long index = availListIndex.get(normCapacity);
        if (val == SINGLE) { // if this is the last node to be removed, register in map
            availListIndex.put(normCapacity, UNAVAILABLE);
        } else { // update availListIndex head to next of cur head
            availLists[prev] = setLower(availLists[prev], NULLPTR); // prev.next = null
            availListIndex.put(normCapacity, setLower(index, prev));
        }
        availLists[pos] = UNAVAILABLE; // mark pos as unavailable
        return prev;
    }

    private void removeElement(int normCapacity, int pos) { // remove node from arbitrary position
        long val = availLists[pos];
        if (isHead(val)) {
            removeHead(normCapacity, pos);
            return;
        } else if (isTail(val)) {
            removeTail(normCapacity, pos);
            return;
        }

        int prev = (int) upper(val);
        int next = (int) lower(val);
        assert prev != NULLPTR && next != NULLPTR : String.valueOf(val); // node is neither head nor tail
        assert prev != LOWER_INT_MASK && next != LOWER_INT_MASK : String.valueOf(val);

        availLists[prev] = setLower(availLists[prev], next); // prev.next = pos.next
        availLists[next] = setUpper(availLists[next], prev); // next.prev = pos.prev
        availLists[pos] = UNAVAILABLE; // pos = null
    }

    private long upper(long val) {
        return val >>> INT_LENGTH;
    }

    private long lower(long val) {
        return val & LOWER_INT_MASK;
    }

    private long setUpper(long val, long upperVal) {
        return  (upperVal << INT_LENGTH) | (val & LOWER_INT_MASK);
    }

    private long setLower(long val, long lowerVal) {
        return (val & UPPER_INT_MASK) | lowerVal;
    }

    private boolean isHead(long val) {
        return upper(val) == NULLPTR;
    }

    private boolean isTail(long val) {
        return lower(val) == NULLPTR;
    }

    private byte value(int id) {
        return (byte) (memoryMap[id] & LOWER_BYTE_MASK);
    }

    private byte depth(int id) {
        short val = memoryMap[id];
        return (byte) (val >>> BYTE_LENGTH);
    }

    private void setVal(int id, byte val) {
        memoryMap[id] = (short) ((memoryMap[id] & UPPER_BYTE_MASK) | val);
    }

    private int runLength(int id) {
        // represents the size in #bytes supported by node 'id' in the tree
        return 1 << (log2ChunkSize - depth(id));
    }

    private int runOffset(int id) {
        // represents the 0-based offset in #bytes from start of the byte-array chunk
        int shift = id ^ (1 << depth(id));
        return shift * runLength(id);
    }

    private int log2(int val) {
        return Integer.SIZE - 1 - Integer.numberOfLeadingZeros(val);
    }

    private int subpageIdx(int memoryMapIdx) {
        return memoryMapIdx ^ maxSubpageAllocs;
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        buf.append("Chunk(");
        buf.append(Integer.toHexString(System.identityHashCode(this)));
        buf.append(": ");
        buf.append(usage());
        buf.append("%, ");
        buf.append(chunkSize - freeBytes);
        buf.append('/');
        buf.append(chunkSize);
        buf.append(')');
        return buf.toString();
    }
}
