// yfs client.  implements FS operations using extent and lock server
#include "yfs_client.h"
#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

yfs_client::yfs_client()
{
  ec = NULL;
  lc = NULL;
}

yfs_client::yfs_client(std::string extent_dst, std::string lock_dst, const char* cert_file)
{
  ec = new extent_client(extent_dst);
  lc = new lock_client(lock_dst);
  if (ec->put(1, "") != extent_protocol::OK)
      printf("error init root dir\n"); // XYB: init root dir
}

/*int
yfs_client::verify(const char* name, unsigned short *uid)
{
    int ret = OK;

    return ret;
}*/


yfs_client::inum
yfs_client::n2i(std::string n)
{
    std::istringstream ist(n);
    unsigned long long finum;
    ist >> finum;
    return finum;
}

std::string
yfs_client::filename(inum inum)
{
    std::ostringstream ost;
    ost << inum;
    return ost.str();
}

bool
yfs_client::isfile(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_FILE) {
        printf("isfile: %lld is a file\n", inum);
        return true;
    } 
    printf("isfile: %lld is a dir\n", inum);
    return false;
}

bool
yfs_client::isdir(inum inum)
{
    //lc->acquire(inum);
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("isdir: error getting attr\n");
        //lc->release(inum);
        return false;
    }

    if (a.type == extent_protocol::T_DIR) {
        printf("isdir: %lld is a dir\n", inum);
        //lc->release(inum);
        return true;
    } 
    printf("isdir: %lld is not a dir\n", inum);
    //lc->release(inum);
    return false;
}

bool
yfs_client::issymlink(inum inum)
{
    //lc->acquire(inum);
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("issymlink: error getting attr\n");
        //lc->release(inum);
        return false;
    }

    if (a.type == extent_protocol::T_SYMLINK) {
        printf("issymlink: %lld is a symlink\n", inum);
        //lc->release(inum);
        return true;
    } 
    printf("issymlink: %lld is not a symlink\n", inum);
    //lc->release(inum);
    return false;
}

int
yfs_client::getfile(inum inum, fileinfo &fin)
{
    int r = OK;

    printf("getfile %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
    printf("getfile %016llx -> sz %llu\n", inum, fin.size);

release:
    return r;
}

int
yfs_client::getdir(inum inum, dirinfo &din)
{
    int r = OK;

    printf("getdir %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;

release:
    return r;
}


#define EXT_RPC(xx) do { \
    if ((xx) != extent_protocol::OK) { \
        printf("EXT_RPC Error: %s:%d \n", __FILE__, __LINE__); \
        r = IOERR; \
        goto release; \
    } \
} while (0)

// Only support set size of attr
int
yfs_client::setattr(inum ino, filestat st, unsigned long toset)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: get the content of inode ino, and modify its content
     * according to the size (<, =, or >) content length.
     */
     lc->acquire(ino);
     size_t size = st.size;
    extent_protocol::attr attr;
    if((r = ec->getattr(ino, attr)) !=  extent_protocol::OK){
        printf("setattr: getattr fail %016llx\n", ino);
        lc->release(ino);
        return r;
    }
    std::string buf;
    if((r = ec->get(ino, buf)) !=  extent_protocol::OK){
        printf("setattr: get fail %016llx\n", ino);
        lc->release(ino);
        return r;
    }
    if(size > attr.size){
        buf += std::string(size - attr.size, '\0');
    }
    else {
        buf = buf.substr(0, size);
    }
    ec->put(ino, buf);
    printf("setattr: succeed %016llx\n", ino);
    lc->release(ino);
    return r;
}


int
yfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: lookup is what you need to check if file exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
     lc->acquire(parent);
    bool found = 0;
    if(lookup(parent, name, found, ino_out) == extent_protocol::IOERR){
        printf("create: lookup fail %016llx\n", parent);
        lc->release(parent);
        return IOERR;
    }
    if(found == 1){
        printf("create: exists %016llx\n", parent);
        lc->release(parent);
        return EXIST;
    }
    if(ec->create(extent_protocol::T_FILE, ino_out) != extent_protocol::OK){
        printf("create: create fail \n");
        lc->release(parent);
        return IOERR;
    }
    std::string buf;
    if(ec->get(parent, buf) !=  extent_protocol::OK){
        printf("create: get fail %016llx\n", parent);
        lc->release(parent);
        return IOERR;
    }
    buf += std::string(name) + "/" + filename(ino_out) + "/";
    if(ec->put(parent, buf) !=  extent_protocol::OK){
        printf("create: put fail %016llx\n", parent);
        lc->release(parent);
        return IOERR;
    }
    printf("create: succeed %016llx\n", ino_out);
    lc->release(parent);
    return r;
}

int
yfs_client::mkdir(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: lookup is what you need to check if directory exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
     lc->acquire(parent);
    bool found = 0;
    if(lookup(parent, name, found, ino_out) == extent_protocol::IOERR){
        printf("mkdir: lookup fail %016llx\n", parent);
        lc->release(parent);
        return IOERR;
    }
    if(found == 1){
        lc->release(parent);
        return EXIST;
    }
    if(ec->create(extent_protocol::T_DIR, ino_out) != extent_protocol::OK){
        printf("mkdir: create fail \n");
        lc->release(parent);
        return IOERR;
    }
    std::string buf;
    if(ec->get(parent, buf) !=  extent_protocol::OK){
        printf("mkdir: get fail %016llx\n", parent);
        lc->release(parent);
        return IOERR;
    }
    buf += std::string(name) + "/" + filename(ino_out) + "/";
    if(ec->put(parent, buf) !=  extent_protocol::OK){
        printf("mkdir: put fail %016llx\n", parent);
        lc->release(parent);
        return IOERR;
    }
    printf("mkdir: succeed %016llx\n", ino_out);
    lc->release(parent);
    return r;
}

int
yfs_client::lookup(inum parent, const char *name, bool &found, inum &ino_out)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: lookup file from parent dir according to name;
     * you should design the format of directory content.
     */
     //lc->acquire(parent);
    found = 0;
    if(!(isdir(parent))){
        printf("lookup: not directory %016llx\n", parent);
        //lc->release(parent);
        return OK;
    }
    std::string buf;
    r = ec->get(parent, buf);
    if(r != extent_protocol::OK){
        printf("lookup: get fail %016llx\n", parent);
        //lc->release(parent);
        return r;
    }
    uint32_t pos = 0;
    size_t size = buf.length();
    uint32_t pos2 = 0;
    std::string fname;
    while(pos < size){
        pos2 = buf.find('/', pos);
        if(pos2 == std::string::npos){
            break;
        }
        fname = buf.substr(pos, pos2-pos);
        if(fname == std::string(name)){
            pos2 += 1;
            pos = buf.find('/', pos2);
            if(pos == std::string::npos){
                printf("lookup: lookup fail\n");
                //lc->release(parent);
                return IOERR;
            }
            ino_out = n2i(buf.substr(pos2, pos-pos2));
            found = true;
            break;
        }
        else{
            pos2 += 1;
            pos = buf.find('/', pos2);
            if(pos == std::string::npos){
                //lc->release(parent);
                return IOERR;
            }
            pos += 1;
        }

    }
    printf("lookup: not found %s\n", name);
    //lc->release(parent);
    return r;
}

int
yfs_client::readdir(inum dir, std::list<dirent> &list)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: you should parse the dirctory content using your defined format,
     * and push the dirents to the list.
     */
     lc->acquire(dir);
    std::string buf;
    if(!(isdir(dir))){
        printf("readdir: not directory %016llx\n", dir);
        lc->release(dir);
        return OK;
    }
    if(ec->get(dir, buf) != extent_protocol::OK){
        printf("readdir: get fail %016llx\n", dir);
        lc->release(dir);
        return r;
    }
    uint32_t pos = 0;
    uint32_t pos2 = 0;
    size_t size = buf.length();
    std::string fname;
    struct dirent tmp;
    while(pos < size){
        pos2 = buf.find('/', pos);
        if(pos2 == std::string::npos){
            break;
        }
        fname = buf.substr(pos, pos2-pos);
        pos2 += 1;
        pos = buf.find('/', pos2);
        if(pos == std::string::npos){
            printf("readdir: read fail %016llx\n", dir);
            lc->release(dir);
            return IOERR;
        }
        tmp.name = fname;
        tmp.inum = n2i(buf.substr(pos2, pos-pos2));
        list.push_back(tmp);
        pos ++;
    }
    lc->release(dir);
    return r;
}

int
yfs_client::read(inum ino, size_t size, off_t off, std::string &data)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: read using ec->get().
     */
     lc->acquire(ino);
    if((r = ec->get(ino, data)) != extent_protocol::OK){
        printf("read: get fail %016llx\n", ino);
        lc->release(ino);
        return IOERR;
    }
    size_t len = data.length();
    if((size_t)off <= len){
        if((size_t)(off + size) <= len)
            data = data.substr(off, size);
        else data = data.substr(off, len-off);
    }
    else data = "";
    printf("read: succeed %016llx\n", ino);
    lc->release(ino);
    return r;
}

int
yfs_client::write(inum ino, size_t size, off_t off, const char *data,
        size_t &bytes_written)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: write using ec->put().
     * when off > length of original file, fill the holes with '\0'.
     */
     lc->acquire(ino);
    std::string buf;
    if((r = ec->get(ino, buf)) != extent_protocol::OK){
        printf("write: get fail %016llx\n", ino);
        lc->release(ino);
        return IOERR;
    }
    size_t len = buf.size();
    if((size_t)off > len){
        bytes_written = off - len + size;
    }
    else{
        bytes_written = size;      
    }
    if((size_t)(off+size) > len){
            buf += std::string(off +size- len, '\0');
        }
    for(size_t i=off; i<off+size;i++){
        buf[i] = data[i-off];
    }
    if((r = ec->put(ino, buf)) != extent_protocol::OK){
        printf("write: put fail %016llx\n", ino);
        lc->release(ino);
        return IOERR;
    }
    printf("write: succeed %016llx\n", ino);
    lc->release(ino);
    return r;
}

int yfs_client::unlink(inum parent,const char *name)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: you should remove the file using ec->remove,
     * and update the parent directory content.
     */
     lc->acquire(parent);
    inum ino_out;
    if(!(isdir(parent))){
        printf("unlink: not dir %016llx\n", parent);
        lc->release(parent);
        return NOENT;
    }
    std::string buf;
    if((r = ec->get(parent, buf)) != extent_protocol::OK){
        printf("unlink: get fail %016llx\n", parent);
        lc->release(parent);
        return r;
    }
    uint32_t pos = 0;
    size_t size = buf.length();
    uint32_t pos2 = 0;
    std::string fname;
    while(pos < size){
        pos2 = buf.find('/', pos);
        if(pos2 == std::string::npos){
            break;
        }
        fname = buf.substr(pos, pos2-pos);
        if(fname == std::string(name)){
            uint32_t tmp = pos;
            pos2 += 1;
            pos = buf.find('/', pos2);
            if(pos == std::string::npos){
                break;
            }
            ino_out = n2i(buf.substr(pos2, pos-pos2));
            lc->acquire(ino_out);
            if(isdir(ino_out)){
                printf("unlink: not file %016llx\n", ino_out);
                lc->release(ino_out);
                lc->release(parent);
                return extent_protocol::NOENT;
            }
            ec->remove(ino_out);
            lc->release(ino_out);
            buf.erase(tmp, buf.substr(pos2, pos-pos2).length() + fname.length() + 2);
            ec->put(parent, buf);
            printf("unlink: succeed %016llx\n", ino_out);
            lc->release(parent);
            return r;
        }
        else{
            pos2 += 1;
            pos = buf.find('/', pos2);
            pos++;
        }

    }
    printf("unlink: no entry %016llx\n", parent);
    r = extent_protocol::NOENT;
    lc->release(parent);
    return r;
}

int yfs_client::readlink(inum ino, std::string &buf)
{
    int r = OK;
    lc->acquire(ino);
    if(!(issymlink(ino))){
        printf("readlink: not symlink %016llx\n", ino);
        lc->release(ino);
        return IOERR;
    }
    if((r = ec->get(ino, buf)) != extent_protocol::OK){
        printf("readlink: get fail %016llx\n", ino);
        lc->release(ino);
        return IOERR;
    }
    printf("readlink: succeed %016llx\n", ino);
    lc->release(ino);
    return r;
}

int yfs_client::symlink(inum parent, const char *name, const char *link, inum &ino_out)
{
    int r = OK;
    lc->acquire(parent);
    std::string buf;
    inum ino;
    if((r = ec->get(parent, buf)) != extent_protocol::OK){
        printf("symlink: get fail %016llx\n", parent);
        lc->release(parent);
        return IOERR;
    }
    bool found = 0;
    if((r = lookup(parent, name, found, ino)) != extent_protocol::OK){
        printf("symlink: lookup fail %016llx\n", parent);
        lc->release(parent);
        return r;
    }
    if(found){
        printf("symlink: exist \n");
        lc->release(parent);
        return EXIST;
    }
    if((r = ec->create(extent_protocol::T_SYMLINK, ino_out)) != extent_protocol::OK){
        printf("symlink: create fail \n");
        lc->release(parent);
        return IOERR;
    }
    if((r = ec->put(ino_out, std::string(link))) != extent_protocol::OK){
        printf("symlink: put link fail %016llx\n", ino_out);
        lc->release(parent);
        return IOERR;
    }
    buf += std::string(name) + "/" + filename(ino_out) + "/";
    if(ec->put(parent, buf) !=  extent_protocol::OK){
        printf("symlink: put fail %016llx\n", parent);
        lc->release(parent);
        return IOERR;
    }
    printf("symlink: succeed %016llx\n",ino_out);
    lc->release(parent);
    return r;
}

void yfs_client::commit()
{
    ec->commit();
}

void yfs_client::undo()
{
    ec->undo();
}

void yfs_client::redo()
{
    ec->redo();
}
