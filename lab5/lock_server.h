// this is the lock server
// the lock client has a similar interface

#ifndef lock_server_h
#define lock_server_h

#include <string>
#include "lock_protocol.h"
#include "lock_client.h"
#include "rpc.h"
#include "pthread.h"
#include "map"

class lock_server {
 private:
  struct thread_state{
  	bool state;
  	pthread_cond_t cond;
    thread_state(){
      state = false;
      pthread_cond_init(&cond, NULL);
    }
  };
  pthread_mutex_t states_mutex;
  std::map<lock_protocol::lockid_t, thread_state> thread_states;
 protected:
  int nacquire;

 public:
  lock_server();
  ~lock_server() {};
  lock_protocol::status stat(int clt, lock_protocol::lockid_t lid, int &);
  lock_protocol::status acquire(int clt, lock_protocol::lockid_t lid, int &);
  lock_protocol::status release(int clt, lock_protocol::lockid_t lid, int &);
};

#endif 







