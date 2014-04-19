#include <search.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <string.h>

typedef struct timeseries_node {
  char* key;
  void* value;
  uint64_t timestamp;
  struct timeseries_node *previous;
} *tsnp;

int compare_nodes_by_key(const void* a, const void* b) {
  return strcmp(((tsnp)a)->key,((tsnp)b)->key);
}

void put(void** table, char* key, void* value, uint64_t timestamp) {
  tsnp provisional_node = malloc(sizeof(struct timeseries_node));
  provisional_node->key=key, provisional_node->value=value, provisional_node->timestamp=timestamp, provisional_node->previous=NULL;
  tsnp pn = NULL, *existing_node = (tsnp*)tfind((void*)provisional_node,table,compare_nodes_by_key);
  if(existing_node) {
    tsnp pn2 = *existing_node;
    while(pn2 && pn2->timestamp > timestamp) pn=pn2, pn2=pn2->previous;
    provisional_node->previous=pn2;
    if(pn) {
      pn->previous=provisional_node;
      return;
    } else {
      tdelete((void*)provisional_node,table,compare_nodes_by_key);
    }
  }
  tsearch((void*)provisional_node,table,compare_nodes_by_key);
}

void* get(void** table, char* key, uint64_t timestamp) {
  struct timeseries_node search_node = { .key=key };
  tsnp* npp = (tsnp*)tfind((void*)&search_node,table,compare_nodes_by_key);
  if(!npp) return NULL;
  tsnp np = *npp;
  while(np && np->timestamp > timestamp) np=np->previous;
  if(!np) return NULL;
  return np->value;
}

int main() {
  void* table[1] = {NULL};
  put(table,"a","1",1000);
  printf("get 'a' at 999: %s\n",(char*)get(table,"a",999));
  printf("get 'a' at 1001: %s\n",(char*)get(table,"a",1001));
  put(table,"a","2",1001);
  put(table,"b","3",1002);
  printf("get 'a' at 1001: %s\n",(char*)get(table,"a",1001));
  printf("get 'a' at 1002: %s\n",(char*)get(table,"a",1002));
  printf("get 'b' at 1003: %s\n",(char*)get(table,"b",1003));
  printf("get 'b' at 1e6: %s\n", (char*)get(table,"b",1e6));
  return 0;
}
