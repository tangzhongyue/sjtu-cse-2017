#include <pthread.h>



#include "inode_manager.h"
#include <ctime>

// disk layer -----------------------------------------

disk::disk()
{
  pthread_t id;
  int ret;
  bzero(blocks, sizeof(blocks));

  ret = pthread_create(&id, NULL, test_daemon, (void*)blocks);
  if(ret != 0)
	  printf("FILE %s line %d:Create pthread error\n", __FILE__, __LINE__);
}

void
disk::read_block(blockid_t id, char *buf)
{
  if (id < 0 || id >= BLOCK_NUM || buf == NULL)
    return;

  memcpy(buf, blocks[id], BLOCK_SIZE);
}

void
disk::write_block(blockid_t id, const char *buf)
{
  if (id < 0 || id >= BLOCK_NUM || buf == NULL)
    return;

  memcpy(blocks[id], buf, BLOCK_SIZE); 
}

// block layer -----------------------------------------
void replication(char c, char *buf){
  for(int i=0; i<8; i++){
    if(c & 0x01){
      buf[i] = 0xFF;
    }
    else{
      buf[i] = 0x00;
    }
    c = c >> 1;
  }
}

char tryCorrect(char *buf){
  char tmp, res=0;
  int k;
  for(int i=0; i<8; i++){
    tmp = buf[i];
    k = !(tmp&0x01) + !(tmp&0x02)+!(tmp&0x04)+!(tmp&0x08)+!(tmp&0x10);
    if(k<=2){
      res = res | (1 << i);
    }
  }
  return res;
}
// Allocate a free disk block.
blockid_t
block_manager::alloc_block()
{
  /*
   * your lab1 code goes here.
   * note: you should mark the corresponding bit in block bitmap when alloc.
   * you need to think about which block you can start to be allocated.

   *hint: use macro IBLOCK and BBLOCK.
          use bit operation.
          remind yourself of the layout of disk.
   */
  pthread_mutex_lock(&bitmap_mutex);
  char buf[BLOCK_SIZE];
  for(unsigned int j = BBLOCK(IBLOCK(sb.ninodes, sb.nblocks));j < BBLOCK(sb.nblocks);j++){
    read_block(j, buf);
    for(unsigned int i = 0;i < BLOCK_SIZE;i++){
      char bits = buf[i];
      for(int k = 0;k < 8;k++){
        if((bits & 0x80) == 0){
          buf[i] = buf[i] | (0x80 >> k);
          write_block(j, buf);
          pthread_mutex_unlock(&bitmap_mutex);
          return ((j - BBLOCK(IBLOCK(sb.ninodes, sb.nblocks))) * BPB + i * 8 + k + IBLOCK(sb.ninodes, sb.nblocks));
        }
        else{
          bits = bits << 1;
        }
      }
    }
  }       
  pthread_mutex_unlock(&bitmap_mutex);
  return -1;
}

void
block_manager::free_block(uint32_t id)
{
  /* 
   * your lab1 code goes here.
   * note: you should unmark the corresponding bit in the block bitmap when free.
   */
   pthread_mutex_lock(&bitmap_mutex);
   char buf[BLOCK_SIZE];
   read_block(BBLOCK(id), buf);
   int posInBuf = (id % BPB) / 8;
   int posInChar = (id % BPB) % 8;
   buf[posInBuf] &= ~(0x80 >> posInChar);
   write_block(BBLOCK(id), buf);
   pthread_mutex_unlock(&bitmap_mutex);
}

// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
block_manager::block_manager()
{
  d = new disk();

  // format the disk
  sb.size = BLOCK_SIZE * BLOCK_NUM/4;
  sb.nblocks = BLOCK_NUM/4;
  sb.ninodes = INODE_NUM;
  sb.version = 0;
  sb.next_inode = 1;
  sb.realinodes = 1;
  pthread_mutex_init(&bitmap_mutex, NULL);
}
inline char parity(char x)
{
    char res = x;
    res = res ^ (res >> 4);
    res = res ^ (res >> 2);
    res = res ^ (res >> 1);
    return res & 1;
}

inline char encode84(char x) 
{
    char res = x & 0x07;
    res |= (x & 0x08) << 1; 
    res |= parity(res & 0x15) << 6;
    res |= parity(res & 0x13) << 5;
    res |= parity(res & 0x07) << 3;
    res |= parity(res) << 7;
    return res;
}

inline bool decode84(char x, char & res) 
{
    bool syndrome = false;
    char tmp = x;
    int fix = 0;
    if (parity(tmp & 0x55)) fix += 1;
    if (parity(tmp & 0x33)) fix += 2;
    if (parity(tmp & 0x0F)) fix += 4;
    if (fix) {
        syndrome = true;
        tmp ^= 1 << (7 - fix);
    }

    if (syndrome && !parity(x)) {
        return false;
    }

    res = tmp & 0x07;
    res |= (tmp & 0x10) >> 1; 
    return true;
}
void
block_manager::read_block(uint32_t id, char *buf)
{
  char rep[BLOCK_SIZE*8];char tmpbuf[8];
  d->read_block(id*8, rep);
  d->read_block(id*8+1, rep+BLOCK_SIZE*1);
  d->read_block(id*8+2, rep+BLOCK_SIZE*2);
  d->read_block(id*8+3, rep+BLOCK_SIZE*3);
  d->read_block(id*8+4, rep+BLOCK_SIZE*4);
  d->read_block(id*8+5, rep+BLOCK_SIZE*5);
  d->read_block(id*8+6, rep+BLOCK_SIZE*6);
  d->read_block(id*8+7, rep+BLOCK_SIZE*7);
  for(int i=0; i<BLOCK_SIZE; i++){
    memcpy( tmpbuf,rep + i*8, 8);
    buf[i] = tryCorrect(tmpbuf);
  }
  write_block(id, buf);
}

void
block_manager::write_block(uint32_t id, const char *buf)
{
  char rep[BLOCK_SIZE*8];
  char tmpbuf[8];
  for(int i=0; i<BLOCK_SIZE; i++){
    replication(buf[i], tmpbuf);
    memcpy(rep + i*8, tmpbuf, 8);
  }
  d->write_block(id*8, rep);
  d->write_block(id*8+1, rep+BLOCK_SIZE*1);
  d->write_block(id*8+2, rep+BLOCK_SIZE*2);
  d->write_block(id*8+3, rep+BLOCK_SIZE*3);
  d->write_block(id*8+4, rep+BLOCK_SIZE*4);
  d->write_block(id*8+5, rep+BLOCK_SIZE*5);
  d->write_block(id*8+6, rep+BLOCK_SIZE*6);
  d->write_block(id*8+7, rep+BLOCK_SIZE*7);
}

// inode layer -----------------------------------------

inode_manager::inode_manager()
{
  bm = new block_manager();
  uint32_t root_dir = alloc_inode(extent_protocol::T_DIR);
  if (root_dir != 1) {
    printf("\tim: error! alloc first inode %d, should be 1\n", root_dir);
    exit(0);
  }
  pthread_mutex_init(&inodes_mutex, NULL);
  pthread_mutex_init(&inodes_get_mutex, NULL);
}

/* Create a new file.
 * Return its inum. */
uint32_t
inode_manager::alloc_inode(uint32_t type)
{
  // use lock to ensure allocation is thread-safe
  printf("alloc inode\n");
  pthread_mutex_lock(&inodes_mutex);
  uint32_t inum;
  uint32_t pos;
  char buf[BLOCK_SIZE];
  inum = bm->sb.next_inode++;
  pos = bm->sb.realinodes++;
  bm->read_block(IBLOCK(pos, bm->sb.nblocks), buf);
  inode_t * ino = (inode_t *)buf;
  ino->commit = -1;
  ino->type = type;
  ino->inum = inum;
  ino->size = 0;
  ino->atime = std::time(0);
  ino->mtime = std::time(0);
  ino->ctime = std::time(0);
  bm->write_block(IBLOCK(pos, bm->sb.nblocks), buf);
  pthread_mutex_unlock(&inodes_mutex);
  return inum;
}


void
inode_manager::free_inode(uint32_t inum)
{
  /* 
   * your lab1 code goes here.
   * note: you need to check if the inode is already a freed one;
   * if not, clear it, and remember to write back to disk.
   * do not forget to free memory if necessary.
   */
   pthread_mutex_lock(&inodes_mutex);
   struct inode *ino = get_inode(inum);
   if(ino == NULL){
    printf("free_inode: no such inode %d\n", inum);
    pthread_mutex_unlock(&inodes_mutex);
    return;
   }
   else{
    ino->type = 0;
    ino->commit = -1;
    put_inode(inum, ino);
    free(ino);
    printf("free_inode: succeed %d\n", inum);
    pthread_mutex_unlock(&inodes_mutex);
    return;
   }
}

#define MIN(a,b) ((a)<(b) ? (a) : (b))
/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
struct inode* 
inode_manager::get_inode(uint32_t inum)
{
  pthread_mutex_lock(&inodes_get_mutex);
  struct inode *ino, *ino_disk, *ino_got;
  char buf[BLOCK_SIZE];
  bool old = 0;
  printf("\tim: get_inode %d\n", inum);

  if (inum < 0 || inum >= INODE_NUM) {
    printf("\tim: inum out of range\n");
    pthread_mutex_unlock(&inodes_get_mutex);
    return NULL;
  }
  
  uint32_t tmpinum = bm->sb.realinodes - 1;
  while(tmpinum > 0){
    bm->read_block(IBLOCK(tmpinum, bm->sb.nblocks), buf);
    ino = (inode_t *)buf;
    if(ino->commit != -1){
      old = 1;
    }
    else if(ino->inum == inum){
      if(ino->type == 0){
        printf("\tim: inode not exist\n");
        pthread_mutex_unlock(&inodes_get_mutex);
        return NULL;
      }
      break;
    }
    tmpinum--;
  }
  if(old){
    char buf2[BLOCK_SIZE];
    tmpinum = bm->sb.realinodes++;
    bm->read_block(IBLOCK(tmpinum, bm->sb.nblocks), buf2);
    ino_disk = (struct inode *)buf2;
    ino_disk->commit = ino->commit;
    ino_disk->type = ino->type;
    ino_disk->inum = ino->inum;
    ino_disk->size = ino->size;
    ino_disk->atime = ino->atime;
    ino_disk->mtime = ino->mtime;
    ino_disk->ctime = ino->ctime;
    char buf3[BLOCK_SIZE];
    char new_indirect[BLOCK_SIZE];
    char old_indirect[BLOCK_SIZE];
    unsigned int block_num = (ino->size + BLOCK_SIZE - 1) / BLOCK_SIZE;
    if (block_num > NDIRECT) {
      bm->read_block(ino->blocks[NDIRECT], old_indirect);
      for (unsigned int i = NDIRECT; i < block_num; ++i) {
        bm->read_block(old_indirect[i - NDIRECT], buf3);
        new_indirect[i - NDIRECT] = bm->alloc_block();
        bm->write_block(new_indirect[i - NDIRECT], buf3);
      }
        ino_disk->blocks[NDIRECT] = bm->alloc_block();
        bm->write_block(ino_disk->blocks[NDIRECT], new_indirect);
      }
      for (unsigned int i = 0; i < MIN(block_num, NDIRECT); ++i) {
        bm->read_block(ino->blocks[i], buf3);
        ino_disk->blocks[i] = bm->alloc_block();
        bm->write_block(ino_disk->blocks[i], buf3);
      }
      bm->write_block(IBLOCK(tmpinum, bm->sb.nblocks), buf2);
      ino = ino_disk;
  }

  ino_got = (struct inode*)malloc(sizeof(*ino_got));
  *ino_got = *ino;
  pthread_mutex_unlock(&inodes_get_mutex);
  return ino_got;
}

void
inode_manager::put_inode(uint32_t inum, struct inode *ino)
{
  pthread_mutex_lock(&inodes_get_mutex);
  char buf[BLOCK_SIZE];
  struct inode *ino_disk, *tmpino;

  printf("\tim: put_inode %d\n", inum);
  if (ino == NULL)
    return;
  uint32_t tmpinum = bm->sb.realinodes - 1;
  while(tmpinum > 0){
    bm->read_block(IBLOCK(tmpinum, bm->sb.nblocks), buf);
    tmpino = (struct inode *)buf;
    if(tmpino->commit != -1){
      return;
    }
    else if(tmpino->inum == inum){
      break;
    }
    tmpinum--;
  }
  bm->read_block(IBLOCK(tmpinum, bm->sb.nblocks), buf);
  ino_disk = (struct inode*)buf;
  *ino_disk = *ino;
  bm->write_block(IBLOCK(tmpinum, bm->sb.nblocks), buf);
  pthread_mutex_unlock(&inodes_get_mutex);
}

/* Get all the data of a file by inum. 
 * Return alloced data, should be freed by caller. */
void
inode_manager::read_file(uint32_t inum, char **buf_out, int *size)
{
  /*
   * your lab1 code goes here.
   * note: read blocks related to inode number inum,
   * and copy them to buf_Out
   */
   printf("read file\n");
  struct inode *ino = get_inode(inum);
  if(ino == NULL){
    printf("read_file: no such inode %d\n", inum);
    return;
  }
  unsigned int totblocks = ino->size / BLOCK_SIZE;
  totblocks += (ino->size % BLOCK_SIZE != 0)? 1 : 0;
  *buf_out = (char *)malloc(totblocks * BLOCK_SIZE );
  unsigned int tmpnum = MIN(totblocks, NDIRECT);
  
  for(unsigned int i = 0; i < tmpnum; i++){
    bm->read_block(ino->blocks[i], *buf_out + i * BLOCK_SIZE);
  }
  if(totblocks > NDIRECT){
    blockid_t blocks[NINDIRECT];
    bm->read_block(ino->blocks[NDIRECT], (char *)blocks);
    for(unsigned int i = NDIRECT; i < totblocks; i++){
      bm->read_block(blocks[i-NDIRECT], *buf_out + i * BLOCK_SIZE);
    }
  }
  *size = ino->size;
  ino->atime = std::time(0);
  ino->ctime = std::time(0);
  put_inode(inum, ino);
  free(ino);
  printf("read_file: succeed %d\n", inum);
}

/* alloc/free blocks if needed */
void
inode_manager::write_file(uint32_t inum, const char *buf, int size)
{
  /*
   * your lab1 code goes here.
   * note: write buf to blocks of inode inum.
   * you need to consider the situation when the size of buf 
   * is larger or smaller than the size of original inode.
   * you should free some blocks if necessary.
   */
   printf("write file\n");
  struct inode *ino = get_inode(inum);
  if(ino == NULL){
    printf("write_file: no such inode %d\n", inum);
    return;
  }
  unsigned int oldBlockNum = ino->size / BLOCK_SIZE;
  if(ino->size % BLOCK_SIZE != 0)oldBlockNum += 1;
  unsigned int newBlockNum = size / BLOCK_SIZE;
  if(size % BLOCK_SIZE != 0) newBlockNum += 1;
  if(newBlockNum > MAXFILE){
    printf("write_file: no space \n");
    return;
  }

  //alloc
  if(newBlockNum > oldBlockNum){
    if(newBlockNum <= NDIRECT){
      for(unsigned int i = oldBlockNum; i < newBlockNum; i++){
        ino->blocks[i] = bm->alloc_block();
      }
    }
    else if(oldBlockNum > NDIRECT){
      blockid_t extraBlocks[NINDIRECT];
      bm->read_block(ino->blocks[NDIRECT], (char *)extraBlocks);
      for(unsigned int i = oldBlockNum; i < newBlockNum; i++){
        extraBlocks[i - NDIRECT] = bm->alloc_block();
      }
      bm->write_block(ino->blocks[NDIRECT], (char *)extraBlocks);
    }
    else{
      for(unsigned int i = oldBlockNum; i < NDIRECT; i++){
        ino->blocks[i] = bm->alloc_block();
      }
      ino->blocks[NDIRECT] = bm->alloc_block();
      blockid_t extraBlocks[NINDIRECT];
      for(unsigned int i = NDIRECT ; i < newBlockNum; i++){
        extraBlocks[i - NDIRECT] = bm->alloc_block();
      }
      bm->write_block(ino->blocks[NDIRECT], (char *)extraBlocks);
    }
  }
  if(newBlockNum < oldBlockNum){
    if(oldBlockNum < NDIRECT){
      for(unsigned int i = newBlockNum; i < oldBlockNum; i++){
        bm->free_block(ino->blocks[i]);
      }
    }
    else if(newBlockNum > NDIRECT){
      blockid_t extraBlocks[NINDIRECT];
      bm->read_block(ino->blocks[NDIRECT], (char *)extraBlocks);
      for(unsigned int i = newBlockNum ; i < oldBlockNum; i++){
        bm->free_block(extraBlocks[i - NDIRECT]);
      }
    }
    else{
      for(unsigned int i = newBlockNum; i < NDIRECT; i++){
        bm->free_block(ino->blocks[i]);
      }
      blockid_t extraBlocks[NINDIRECT];
      bm->read_block(ino->blocks[NDIRECT], (char *)extraBlocks);
      for(unsigned int i = newBlockNum ; i < oldBlockNum; i++){
        bm->free_block(extraBlocks[i - NDIRECT]);
      }
    }
  }
  //write
  char block[BLOCK_SIZE];
  char indirect[BLOCK_SIZE];
      int cur = 0;
  for (int i = 0; i < NDIRECT && cur < size; ++i) {
    if (size - cur > BLOCK_SIZE) {
      bm->write_block(ino->blocks[i], buf + cur);
      cur += BLOCK_SIZE;
    } else {
      int len = size - cur;
      memcpy(block, buf + cur, len);
      bm->write_block(ino->blocks[i], block);
      cur += len;
    }
  }

  if (cur < size) {
    bm->read_block(ino->blocks[NDIRECT], indirect);
    for (unsigned int i = 0; i < NINDIRECT && cur < size; ++i) {
      blockid_t ix = *((blockid_t *)indirect + i);
      if (size - cur > BLOCK_SIZE) {
        bm->write_block(ix, buf + cur);
        cur += BLOCK_SIZE;
      } else {
        int len = size - cur;
        memcpy(block, buf + cur, len);
        bm->write_block(ix, block);
        cur += len;
      }
    }
  }

  //free
  
  ino->size = size;
  //ino->atime = std::time(0);
  ino->mtime = std::time(0);
  ino->ctime = std::time(0);
  put_inode(inum, ino);
  free(ino);
  printf("write_file: succeed %d\n", inum);
}

void
inode_manager::getattr(uint32_t inum, extent_protocol::attr &a)
{
  /*
   * your lab1 code goes here.
   * note: get the attributes of inode inum.
   * you can refer to "struct attr" in extent_protocol.h
   */
   struct inode *ino = get_inode(inum);
   if(ino != NULL){
   a.type = ino->type;
   a.atime = ino->atime;
   a.ctime = ino->ctime;
   a.mtime = ino->mtime;
   a.size = ino->size;
   free(ino);
   printf("getattr: succeed %d\n", inum);
  }
  else printf("getattr: no such inode %d\n", inum);
}

void
inode_manager::remove_file(uint32_t inum)
{
  /*
   * your lab1 code goes here
   * note: you need to consider about both the data block and inode of the file
   * do not forget to free memory if necessary.
   */
   pthread_mutex_lock(&inodes_mutex);
   struct inode *ino = get_inode(inum);
   if(ino == NULL){
    printf("remove_file: no such inode %d\n", inum);
    pthread_mutex_unlock(&inodes_mutex);
    return;
   }
   unsigned int size = ino->size;
   unsigned int totblocks = size/BLOCK_SIZE;
   totblocks += (size % BLOCK_SIZE != 0)? 1 : 0;
   unsigned int min = MIN(totblocks, NDIRECT);
   for(unsigned int i = 0; i < min; i++){
      bm->free_block(ino->blocks[i]);
    }
   if(totblocks > NDIRECT){
    blockid_t blocks[NINDIRECT];
    bm->read_block(ino->blocks[NDIRECT], (char *)blocks);
    for(unsigned int i = NDIRECT; i < totblocks; i++){
      bm->free_block(blocks[i-NDIRECT]);
    }
   }
   free(ino);
   struct inode *ino1 = get_inode(inum);
   if(ino1 == NULL){
    printf("free_inode in remove: no such inode %d\n", inum);
   }
   else{
    ino1->type = 0;
    ino1->commit = -1;
    put_inode(inum, ino1);
    free(ino1);
    printf("free_inode in remove: succeed %d\n", inum);
   }
   printf("remove_file: succeed %d\n", inum);
   pthread_mutex_unlock(&inodes_mutex);
}

void
inode_manager::commit()
{
  uint32_t pos;
  char buf[BLOCK_SIZE];
  pos = bm->sb.realinodes++;
  bm->read_block(IBLOCK(pos, bm->sb.nblocks), buf);
  inode_t * ino = (inode_t *)buf;
  ino->commit = bm->sb.version++;
  bm->write_block(IBLOCK(pos, bm->sb.nblocks), buf);
}

void
inode_manager::undo()
{
  char buf[BLOCK_SIZE];
  --bm->sb.version;
  while (1) {
    printf("undo\n");
      bm->read_block(IBLOCK(--bm->sb.realinodes, bm->sb.nblocks), buf);
      inode_t * ino = (inode_t *)buf;
      if (ino->commit == (short)bm->sb.version) {
          return;
      }
  }
}

void
inode_manager::redo()
{
  char buf[BLOCK_SIZE];
  ++bm->sb.version;
  while (1) {
    printf("redo\n");
      bm->read_block(IBLOCK(bm->sb.realinodes, bm->sb.nblocks), buf);
      inode_t * ino = (inode_t *)buf;
      if (ino->commit == (short)bm->sb.version) {
          return;
      }
      ++bm->sb.realinodes;
  }
}
