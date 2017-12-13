#include "inode_manager.h"
#include <ctime>

// disk layer -----------------------------------------

disk::disk()
{
  bzero(blocks, sizeof(blocks));
}

void
disk::read_block(blockid_t id, char *buf)
{
  /*
   *your lab1 code goes here.
   *if id is smaller than 0 or larger than BLOCK_NUM 
   *or buf is null, just return.
   *put the content of target block into buf.
   *hint: use memcpy
  */
   if(id < 0 || id > BLOCK_NUM || buf == NULL){
    return;
   }
   memcpy(buf, blocks[id], BLOCK_SIZE);
}

void
disk::write_block(blockid_t id, const char *buf)
{
  /*
   *your lab1 code goes here.
   *hint: just like read_block
  */
   if(id < 0 || id > BLOCK_NUM || buf == NULL){
    return;
   }
   memcpy(blocks[id], buf, BLOCK_SIZE);
}

// block layer -----------------------------------------

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
  char buf[BLOCK_SIZE];
  for(unsigned int j = BBLOCK(IBLOCK(sb.ninodes, sb.nblocks));j < BBLOCK(sb.nblocks);j++){
    read_block(j, buf);
    for(unsigned int i = 0;i < BLOCK_SIZE;i++){
      char bits = buf[i];
      for(int k = 0;k < 8;k++){
        if((bits & 0x80) == 0){
          buf[i] = buf[i] | (0x80 >> k);
          write_block(j, buf);
          return ((j - BBLOCK(IBLOCK(sb.ninodes, sb.nblocks))) * BPB + i * 8 + k + IBLOCK(sb.ninodes, sb.nblocks));
        }
        else{
          bits = bits << 1;
        }
      }
    }
  }       
  return -1;
}

void
block_manager::free_block(uint32_t id)
{
  /* 
   * your lab1 code goes here.
   * note: you should unmark the corresponding bit in the block bitmap when free.
   */
   char buf[BLOCK_SIZE];
   read_block(BBLOCK(id), buf);
   int posInBuf = (id % BPB) / 8;
   int posInChar = (id % BPB) % 8;
   buf[posInBuf] &= ~(0x80 >> posInChar);
   write_block(BBLOCK(id), buf);
}

// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
block_manager::block_manager()
{
  d = new disk();

  // format the disk
  sb.size = BLOCK_SIZE * BLOCK_NUM;
  sb.nblocks = BLOCK_NUM;
  sb.ninodes = INODE_NUM;

}

void
block_manager::read_block(uint32_t id, char *buf)
{
  d->read_block(id, buf);
}

void
block_manager::write_block(uint32_t id, const char *buf)
{
  d->write_block(id, buf);
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
}

/* Create a new file.
 * Return its inum. */
uint32_t
inode_manager::alloc_inode(uint32_t type)
{
  /* 
   * your lab1 code goes here.
   * note: the normal inode block should begin from the 2nd inode block.
   * the 1st is used for root_dir, see inode_manager::inode_manager().
    
   * if you get some heap memory, do not forget to free it.
   */
  char buf[BLOCK_SIZE];
  struct inode *ino = (struct inode*)malloc(sizeof(struct inode)),
  *inotmp;
  ino->type = type;
  ino->size = 0;
  ino->atime = std::time(0);
  ino->mtime = std::time(0);
  ino->ctime = std::time(0);
  for(uint32_t i = 1; i <= bm->sb.ninodes; i++){
    bm->read_block(IBLOCK(i, bm->sb.nblocks), buf);
    inotmp = (struct inode*)buf + i%IPB;
    if(inotmp->type == 0){
      put_inode(i, ino);
      free(ino);
      return i;
    }
  }
  free(ino);
  return -1;
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
   struct inode *ino = get_inode(inum);
   if(ino == NULL){
    return;
   }
   else{
    ino->type = 0;
    put_inode(inum, ino);
    free(ino);
    return;
   }
}


/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
struct inode* 
inode_manager::get_inode(uint32_t inum)
{
  struct inode *ino, *ino_disk;
  char buf[BLOCK_SIZE];

  printf("\tim: get_inode %d\n", inum);

  if (inum < 0 || inum >= INODE_NUM) {
    printf("\tim: inum out of range\n");
    return NULL;
  }

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  // printf("%s:%d\n", __FILE__, __LINE__);

  ino_disk = (struct inode*)buf + inum%IPB;
  if (ino_disk->type == 0) {
    printf("\tim: inode not exist\n");
    return NULL;
  }

  ino = (struct inode*)malloc(sizeof(struct inode));
  *ino = *ino_disk;

  return ino;
}

void
inode_manager::put_inode(uint32_t inum, struct inode *ino)
{
  char buf[BLOCK_SIZE];
  struct inode *ino_disk;

  printf("\tim: put_inode %d\n", inum);
  if (ino == NULL)
    return;
  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  ino_disk = (struct inode*)buf + inum%IPB;
  *ino_disk = *ino;
  bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
}

#define MIN(a,b) ((a)<(b) ? (a) : (b))

/* Get all the data of a file by inum. 
 * Return alloced data, should be freed by caller. */
void
inode_manager::read_file(uint32_t inum, char **buf_out, int *size)
{
  /*
   * your lab1 code goes here.
   * note: read blocks related to inode number inum,
   * and copy them to buf_out
   */
  struct inode *ino = get_inode(inum);
  if(ino == NULL){
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
  struct inode *ino = get_inode(inum);
  if(ino == NULL){
    return;
  }
  unsigned int oldBlockNum = ino->size / BLOCK_SIZE;
  oldBlockNum += (ino->size % BLOCK_SIZE != 0)? 1 : 0;
  unsigned int newBlockNum = size / BLOCK_SIZE;
  newBlockNum += (size % BLOCK_SIZE != 0)? 1 : 0;
  if(newBlockNum > MAXFILE){
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
  //write
  if(newBlockNum <= NDIRECT){
      for(unsigned int i = 0; i < newBlockNum; i++){
        bm->write_block(ino->blocks[i], buf + i * BLOCK_SIZE);
      }
  }
  else {
    for(unsigned int i = 0; i < NDIRECT; i++){
      bm->write_block(ino->blocks[i], buf + i * BLOCK_SIZE);
    }
    blockid_t extraBlocks[NINDIRECT];
    bm->read_block(ino->blocks[NDIRECT], (char *)extraBlocks);
    for(unsigned int i = NDIRECT ; i < newBlockNum; i++){
      bm->write_block(extraBlocks[i - NDIRECT], buf + i * BLOCK_SIZE);
    }
  }
  //free
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
  ino->size = size;
  ino->atime = std::time(0);
  ino->mtime = std::time(0);
  ino->ctime = std::time(0);
  put_inode(inum, ino);
  free(ino);
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
  }
}

void
inode_manager::remove_file(uint32_t inum)
{
  /*
   * your lab1 code goes here
   * note: you need to consider about both the data block and inode of the file
   * do not forget to free memory if necessary.
   */
   struct inode *ino = get_inode(inum);
   if(ino == NULL){
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
   free_inode(inum);
}
