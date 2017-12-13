// the lock server implementation

#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>

lock_server::lock_server():
  nacquire (0)
{
  pthread_mutex_init(&states_mutex, NULL);
}

lock_protocol::status
lock_server::stat(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  printf("stat request from clt %d\n", clt);
  r = nacquire;
  return ret;
}

lock_protocol::status
lock_server::acquire(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  // Your lab4 code goes here
  pthread_mutex_lock(&states_mutex);
  printf("acquire %llx from clt %d\n", lid, clt);
  if (thread_states.find(lid) != thread_states.end()) {
    while (thread_states[lid].state == true) {
        pthread_cond_wait(&thread_states[lid].cond, &states_mutex);
    }
  }
  thread_states[lid].state = true;
  printf("acquire %llx from clt %d end\n", lid, clt);
  pthread_mutex_unlock(&states_mutex);
  return ret;
}

lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  // Your lab4 code goes here
  pthread_mutex_lock(&states_mutex);
  printf("release %llx from clt %d\n", lid, clt);
  if(thread_states.find(lid) != thread_states.end()) {
    thread_states[lid].state = false;
    pthread_cond_signal(&thread_states[lid].cond);
    printf("release %llx from clt %d end\n", lid, clt);
    pthread_mutex_unlock(&states_mutex);
  }
  else{
    ret = lock_protocol::IOERR;
    printf("release %llx from clt %d error\n", lid, clt);
    pthread_mutex_unlock(&states_mutex);
  }
  return ret;
}
