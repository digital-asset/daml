#include <grpc/grpc.h>
#include <grpc/byte_buffer.h>
#include <grpc/byte_buffer_reader.h>
#include <grpc/grpc_security.h>
#include <grpc/impl/codegen/grpc_types.h>
#include <grpc/impl/codegen/compression_types.h>
#include <grpc/slice.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <grpc_haskell.h>

void grpc_haskell_free(char *debugMsg, void *ptr){
  #ifdef GRPC_HASKELL_DEBUG
  printf("C wrapper: %s: freeing ptr: %p\n", debugMsg, ptr);
  #endif
  free(ptr);
}

grpc_event *grpc_completion_queue_next_(grpc_completion_queue *cq,
                                        gpr_timespec *deadline,
                                        void *reserved) {
  grpc_event *toReturn = malloc(sizeof(grpc_event));
  *toReturn = grpc_completion_queue_next(cq, *deadline, reserved);
  return toReturn;
}

grpc_event *grpc_completion_queue_pluck_(grpc_completion_queue *cq, void *tag,
                                         gpr_timespec *deadline,
                                         void *reserved) {
  grpc_event *toReturn = malloc(sizeof(grpc_event));
  *toReturn = grpc_completion_queue_pluck(cq, tag, *deadline, reserved);
  return toReturn;
}

grpc_call *grpc_channel_create_call_(grpc_channel *channel,
                                     grpc_call *parent_call,
                                     uint32_t propagation_mask,
                                     grpc_completion_queue *completion_queue,
                                     const char *method, const char *host,
                                     gpr_timespec *deadline, void *reserved) {
  grpc_slice method_slice = grpc_slice_from_copied_string(method);
  grpc_slice host_slice = grpc_slice_from_copied_string(host);
  return grpc_channel_create_call(channel, parent_call, propagation_mask,
                                       completion_queue, method_slice,
                                       &host_slice, *deadline, reserved);
}

grpc_slice* grpc_slice_malloc_(size_t len){
  grpc_slice* retval = malloc(sizeof(grpc_slice));
  *retval = grpc_slice_malloc(len);
  return retval;
}

size_t grpc_slice_length_(grpc_slice *slice){
  return GRPC_SLICE_LENGTH(*slice);
}

uint8_t *grpc_slice_start_(grpc_slice *slice){
  return GRPC_SLICE_START_PTR(*slice);
}


grpc_slice* grpc_slice_from_copied_string_(const char* source){
  grpc_slice* retval = malloc(sizeof(grpc_slice));
  *retval = grpc_slice_from_copied_string(source);
  return retval;
}

grpc_slice* grpc_slice_from_copied_buffer_(const char *source, size_t len){
  grpc_slice* retval = malloc(sizeof(grpc_slice));
  //note: 'grpc_slice_from_copied_string' handles allocating space for 'source'.
  *retval = grpc_slice_from_copied_buffer(source, len);
  return retval;
}

void grpc_slice_unref_(grpc_slice* slice){
  grpc_slice_unref(*slice);
}

void free_slice(grpc_slice *slice){
  grpc_slice_unref(*slice);
  grpc_haskell_free("free_slice", slice);
}

grpc_byte_buffer **create_receiving_byte_buffer(){
  grpc_byte_buffer **retval = malloc(sizeof(grpc_byte_buffer*));
  *retval = NULL;
  return retval;
}

void destroy_receiving_byte_buffer(grpc_byte_buffer **bb){
  grpc_byte_buffer_destroy(*bb);
  grpc_haskell_free("destroy_receiving_byte_buffer", bb);
}

grpc_byte_buffer_reader *byte_buffer_reader_create(grpc_byte_buffer *buffer){
  grpc_byte_buffer_reader *reader = malloc(sizeof(grpc_byte_buffer_reader));
  grpc_byte_buffer_reader_init(reader, buffer);
  return reader;
}

void byte_buffer_reader_destroy(grpc_byte_buffer_reader *reader){
  grpc_byte_buffer_reader_destroy(reader);
  grpc_haskell_free("byte_buffer_reader_destroy", reader);
}

grpc_slice *grpc_byte_buffer_reader_readall_(grpc_byte_buffer_reader *reader){
  grpc_slice *retval = malloc(sizeof(grpc_slice));
  *retval = grpc_byte_buffer_reader_readall(reader);
  return retval;
}

void timespec_destroy(gpr_timespec* t){
  grpc_haskell_free("timespec_destroy", t);
}

gpr_timespec* gpr_inf_future_(gpr_clock_type t){
  gpr_timespec *retval = malloc(sizeof(gpr_timespec));
  *retval = gpr_inf_future(t);
  return retval;
}

gpr_timespec* gpr_now_(gpr_clock_type t){
  gpr_timespec *retval = malloc(sizeof(gpr_timespec));
  *retval = gpr_now(t);
  return retval;
}

int32_t gpr_time_to_millis_(gpr_timespec* t){
  return gpr_time_to_millis(*t);
}

gpr_timespec* seconds_to_deadline(int64_t seconds){
  gpr_timespec *retval = malloc(sizeof(gpr_timespec));
  *retval = gpr_time_add(gpr_now(GPR_CLOCK_MONOTONIC),
                         gpr_time_from_millis(seconds * 1e3, GPR_TIMESPAN));
  return retval;
}

gpr_timespec* millis_to_deadline(int64_t millis){
  gpr_timespec *retval = malloc(sizeof(gpr_timespec));
  *retval = gpr_time_add(gpr_now(GPR_CLOCK_MONOTONIC),
                         gpr_time_from_micros(millis * 1e3, GPR_TIMESPAN));
  return retval;
}

gpr_timespec* infinite_deadline(){
  gpr_timespec *retval = malloc(sizeof(gpr_timespec));
  *retval = gpr_inf_future(GPR_CLOCK_MONOTONIC);
  return retval;
}

gpr_timespec* convert_clock_type(gpr_timespec *t, gpr_clock_type to){
  gpr_timespec *retval = malloc(sizeof(gpr_timespec));
  *retval = gpr_convert_clock_type(*t, to);
  return retval;
}

grpc_metadata_array** metadata_array_create(){
  grpc_metadata_array **retval = malloc(sizeof(grpc_metadata_array*));
  *retval = malloc(sizeof(grpc_metadata_array));
  grpc_metadata_array_init(*retval);
  #ifdef GRPC_HASKELL_DEBUG
  printf("C wrapper: metadata_array_create debug: %p %p %p\n", retval, *retval,
         (*retval)->metadata);
  #endif
  return retval;
}

void metadata_array_destroy(grpc_metadata_array **arr){
  grpc_metadata_array_destroy(*arr);
  grpc_haskell_free("metadata_array_destroy1", *arr);
  grpc_haskell_free("metadata_array_destroy1", arr);
}

grpc_metadata* metadata_alloc(size_t n){
  grpc_metadata *retval = calloc(n,sizeof(grpc_metadata));
  return retval;
}

void metadata_free(grpc_metadata* m){
  grpc_haskell_free("metadata_free", m);
}

void set_metadata_key_val(char *key, char *val, size_t val_len,
                          grpc_metadata *arr, size_t i){
  grpc_metadata *p = arr + i;
  p->key = grpc_slice_from_copied_string(key);
  p->value = grpc_slice_from_copied_buffer(val,val_len);
}

grpc_slice* get_metadata_key(grpc_metadata *arr, size_t i){
  grpc_metadata *p = arr + i;
  return &p->key;
}

grpc_slice* get_metadata_val(grpc_metadata *arr, size_t i){
  grpc_metadata *p = arr + i;
  return &(p->value);
}

grpc_op* op_array_create(size_t n){
  grpc_op* ops = malloc(n*sizeof(grpc_op));
  memset(ops, 0, n*sizeof(grpc_op));
  return ops;
}

void op_array_destroy(grpc_op* op_array, size_t n){
  #ifdef GRPC_HASKELL_DEBUG
  printf("C wrapper: entered op_array_destroy\n");
  #endif
  for(int i = 0; i < n; i++){
    grpc_op* op = op_array + i;
    switch (op->op) {
      case GRPC_OP_SEND_INITIAL_METADATA:
      metadata_free(op->data.send_initial_metadata.metadata);
      break;
      case GRPC_OP_SEND_MESSAGE:
      grpc_byte_buffer_destroy(op->data.send_message.send_message);
      break;
      case GRPC_OP_SEND_CLOSE_FROM_CLIENT:
      break;
      case GRPC_OP_SEND_STATUS_FROM_SERVER:
      grpc_haskell_free("op_array_destroy: GRPC_OP_SEND_STATUS_FROM_SERVER",
                        op->data.send_status_from_server.trailing_metadata);
      grpc_haskell_free("op_array_destroy: GRPC_OP_SEND_STATUS_FROM_SERVER",
                      (char*)(op->data.send_status_from_server.status_details));
      break;
      case GRPC_OP_RECV_INITIAL_METADATA:
      break;
      case GRPC_OP_RECV_MESSAGE:
      break;
      case GRPC_OP_RECV_STATUS_ON_CLIENT:
      break;
      case GRPC_OP_RECV_CLOSE_ON_SERVER:
      break;
    }
  }
  grpc_haskell_free("op_array_destroy", op_array);
}

void op_send_initial_metadata(grpc_op *op_array, size_t i,
                              grpc_metadata *arr, size_t n_metadata){
  grpc_op *op = op_array + i;
  op->op = GRPC_OP_SEND_INITIAL_METADATA;
  op->data.send_initial_metadata.count = n_metadata;
  op->data.send_initial_metadata.metadata
    = malloc(n_metadata*sizeof(grpc_metadata));
  memcpy(op->data.send_initial_metadata.metadata, arr,
         n_metadata*sizeof(grpc_metadata));
  op->flags = 0;
  op->reserved = NULL;
}

void op_send_initial_metadata_empty(grpc_op *op_array, size_t i){
  grpc_op *op = op_array + i;
  op->op = GRPC_OP_SEND_INITIAL_METADATA;
  op->data.send_initial_metadata.count = 0;
  op->data.send_initial_metadata.metadata = malloc(0*sizeof(grpc_metadata));
  op->flags = 0;
  op->reserved = NULL;
}

void op_send_message(grpc_op *op_array, size_t i,
                         grpc_byte_buffer *payload){
  grpc_op *op = op_array + i;
  op->op = GRPC_OP_SEND_MESSAGE;
  op->data.send_message.send_message = grpc_byte_buffer_copy(payload);
  op->flags = 0;
  op->reserved = NULL;
}

void op_send_close_client(grpc_op *op_array, size_t i){
  grpc_op *op = op_array + i;
  op->op = GRPC_OP_SEND_CLOSE_FROM_CLIENT;
  op->flags = 0;
  op->reserved = NULL;
}

void op_recv_initial_metadata(grpc_op *op_array, size_t i,
                              grpc_metadata_array** arr){
  grpc_op *op = op_array + i;
  op->op = GRPC_OP_RECV_INITIAL_METADATA;
  op->data.recv_initial_metadata.recv_initial_metadata = *arr;
  op->flags = 0;
  op->reserved = NULL;
}

void op_recv_message(grpc_op *op_array, size_t i,
                     grpc_byte_buffer **payload_recv){
  grpc_op *op = op_array + i;
  op->op = GRPC_OP_RECV_MESSAGE;
  op->data.recv_message.recv_message = payload_recv;
  op->flags = 0;
  op->reserved = NULL;
}

void op_recv_status_client(grpc_op *op_array, size_t i,
                           grpc_metadata_array** arr,
                           grpc_status_code* status,
                           grpc_slice* details){
  grpc_op *op = op_array + i;
  op->op = GRPC_OP_RECV_STATUS_ON_CLIENT;
  op->data.recv_status_on_client.trailing_metadata = *arr;
  op->data.recv_status_on_client.status = status;
  op->data.recv_status_on_client.status_details = details;
  op->flags = 0;
  op->reserved = NULL;
}

void op_recv_close_server(grpc_op *op_array, size_t i, int *was_cancelled){
  grpc_op *op = op_array + i;
  op->op = GRPC_OP_RECV_CLOSE_ON_SERVER;
  op->data.recv_close_on_server.cancelled = was_cancelled;
  op->flags = 0;
  op->reserved = NULL;
}

void op_send_status_server(grpc_op *op_array, size_t i,
                           size_t metadata_count, grpc_metadata* m,
                           grpc_status_code status, grpc_slice *details){
  grpc_op *op = op_array + i;
  op->op = GRPC_OP_SEND_STATUS_FROM_SERVER;
  op->data.send_status_from_server.trailing_metadata_count = metadata_count;
  op->data.send_status_from_server.trailing_metadata
    = malloc(sizeof(grpc_metadata)*metadata_count);
  memcpy(op->data.send_status_from_server.trailing_metadata, m,
         metadata_count*sizeof(grpc_metadata));
  op->data.send_status_from_server.status = status;
  op->data.send_status_from_server.status_details = details;
  op->flags = 0;
  op->reserved = NULL;
}

grpc_status_code* create_status_code_ptr(){
  grpc_status_code* retval = malloc(sizeof(grpc_status_code));
  #ifdef GRPC_HASKELL_DEBUG
  printf("C wrapper: create_status_code_ptr debug: %p\n", retval);
  #endif
  return retval;
}

grpc_status_code deref_status_code_ptr(grpc_status_code* p){
  return *p;
}

void destroy_status_code_ptr(grpc_status_code* p){
  grpc_haskell_free("destroy_status_code_ptr", p);
}

grpc_call_details* create_call_details(){
  grpc_call_details* retval = malloc(sizeof(grpc_call_details));
  grpc_call_details_init(retval);
  return retval;
}

void destroy_call_details(grpc_call_details* cd){
  grpc_call_details_destroy(cd);
  grpc_haskell_free("destroy_call_details", cd);
}

void grpc_channel_watch_connectivity_state_(grpc_channel *channel,
                                            grpc_connectivity_state
                                            last_observed_state,
                                            gpr_timespec* deadline,
                                            grpc_completion_queue *cq,
                                            void *tag){
  grpc_channel_watch_connectivity_state(channel, last_observed_state, *deadline,
                                        cq, tag);
}

grpc_metadata* metadata_array_get_metadata(grpc_metadata_array* arr){
  return arr->metadata;
}

void metadata_array_set_metadata(grpc_metadata_array* arr, grpc_metadata* meta){
  arr->metadata = meta;
  //NOTE: we assume count == capacity because that's how the 'createMetadata'
  //Haskell function works. It isn't safe to call this function if the
  //metadata was created in some other way.
  size_t n = sizeof(meta);
  arr->count = n;
  arr->capacity = n;
}

size_t metadata_array_get_count(grpc_metadata_array* arr){
  return arr->count;
}

size_t metadata_array_get_capacity(grpc_metadata_array* arr){
  return arr->capacity;
}

grpc_call* grpc_channel_create_registered_call_(
  grpc_channel *channel, grpc_call *parent_call, uint32_t propagation_mask,
  grpc_completion_queue *completion_queue, void *registered_call_handle,
  gpr_timespec *deadline, void *reserved){
    #ifdef GRPC_HASKELL_DEBUG
    printf("calling grpc_channel_create_registered_call with deadline %p\n",
           deadline);
    #endif
    return grpc_channel_create_registered_call(channel, parent_call,
             propagation_mask, completion_queue, registered_call_handle,
             *deadline, reserved);
}

grpc_slice* call_details_get_method(grpc_call_details* details){
  return &details->method;
}

grpc_slice* call_details_get_host(grpc_call_details* details){
  return &details->host;
}

gpr_timespec* call_details_get_deadline(grpc_call_details* details){
  return &(details->deadline);
}

void* grpc_server_register_method_(
  grpc_server* server, const char* method,
  const char* host, grpc_server_register_method_payload_handling payload_handling ){
  return grpc_server_register_method(server, method, host, payload_handling, 0);
}

grpc_arg* create_arg_array(size_t n){
  return malloc(sizeof(grpc_arg)*n);
}

//Converts our enum into real GRPC #defines. c2hs workaround.
char* translate_arg_key(enum supported_arg_key key){
  switch (key) {
    case compression_algorithm_key:
      return GRPC_COMPRESSION_CHANNEL_DEFAULT_ALGORITHM;
    case compression_level_key:
      return GRPC_COMPRESSION_CHANNEL_DEFAULT_LEVEL;
    case user_agent_prefix_key:
      return GRPC_ARG_PRIMARY_USER_AGENT_STRING;
    case user_agent_suffix_key:
      return GRPC_ARG_SECONDARY_USER_AGENT_STRING;
    case max_receive_message_length_key:
      return GRPC_ARG_MAX_RECEIVE_MESSAGE_LENGTH;
    case max_metadata_size_key:
      return GRPC_ARG_MAX_METADATA_SIZE;
    default:
      return "unknown_arg_key";
  }
}

void create_string_arg(grpc_arg* args, size_t i,
                       enum supported_arg_key key, char* value){
  grpc_arg* arg = args+i;
  arg->type = GRPC_ARG_STRING;
  arg->key = translate_arg_key(key);
  char* storeValue = malloc(sizeof(char)*strlen(value));
  arg->value.string = strcpy(storeValue, value);
}

void create_int_arg(grpc_arg* args, size_t i,
                    enum supported_arg_key key, int value){
  grpc_arg* arg = args+i;
  arg->type = GRPC_ARG_INTEGER;
  arg->key = translate_arg_key(key);
  arg->value.integer = value;
}

//Destroys an arg array of the given length. NOTE: the args in the arg array
//MUST have been created by the create_*_arg functions above!
void destroy_arg_array(grpc_arg* args, size_t n){
  for(int i = 0; i < n; i++){
    grpc_arg* arg = args+i;
    if(arg->type == GRPC_ARG_STRING){
      free(arg->value.string);
    }
  }
  free(args);
}

grpc_auth_property_iterator* grpc_auth_context_property_iterator_(
  const grpc_auth_context* ctx){

  grpc_auth_property_iterator* i = malloc(sizeof(grpc_auth_property_iterator));
  *i = grpc_auth_context_property_iterator(ctx);
  return i;
}

grpc_server_credentials* ssl_server_credentials_create_internal(
  const char* pem_root_certs, const char* pem_key, const char* pem_cert,
  grpc_ssl_client_certificate_request_type force_client_auth){

  grpc_ssl_pem_key_cert_pair pair = {pem_key, pem_cert};
  grpc_server_credentials* creds = grpc_ssl_server_credentials_create_ex(
    pem_root_certs, &pair, 1, force_client_auth, NULL);
  return creds;
}

grpc_channel_credentials* grpc_ssl_credentials_create_internal(
  const char* pem_root_certs, const char* pem_key, const char* pem_cert){

  grpc_channel_credentials* creds;
  if(pem_key && pem_cert){
    grpc_ssl_pem_key_cert_pair pair = {pem_key, pem_cert};
    creds = grpc_ssl_credentials_create(pem_root_certs, &pair, NULL, NULL);
  }
  else{
    creds = grpc_ssl_credentials_create(pem_root_certs, NULL, NULL, NULL);
  }
  return creds;
}

void grpc_server_credentials_set_auth_metadata_processor_(
  grpc_server_credentials* creds, grpc_auth_metadata_processor* p){

  grpc_server_credentials_set_auth_metadata_processor(creds, *p);
}

grpc_auth_metadata_processor* mk_auth_metadata_processor(
  void (*process)(void *state, grpc_auth_context *context,
                  const grpc_metadata *md, size_t num_md,
                  grpc_process_auth_metadata_done_cb cb, void *user_data)){

  //TODO: figure out when to free this.
  grpc_auth_metadata_processor* p = malloc(sizeof(grpc_auth_metadata_processor));
  p->process = process;
  p->destroy = NULL;
  p->state = NULL;
  return p;
}

grpc_call_credentials* grpc_metadata_credentials_create_from_plugin_(
  grpc_metadata_credentials_plugin* plugin){

  return grpc_metadata_credentials_create_from_plugin(*plugin, NULL);
}

//This is a hack to work around GHC being unable to deal with raw struct params.
//This callback is registered as the get_metadata callback for the call, and its
//only job is to cast the void* state pointer to the correct function pointer
//type and call the Haskell function with it.
int metadata_dispatcher(void *state,
 grpc_auth_metadata_context context,
 grpc_credentials_plugin_metadata_cb cb,
 void *user_data,
 grpc_metadata creds_md[GRPC_METADATA_CREDENTIALS_PLUGIN_SYNC_MAX],
 size_t *num_creds_md,
 grpc_status_code *status,
 const char ** error_details) {
  ((haskell_get_metadata*)state)(&context, cb, user_data);
  return 0;
}

grpc_metadata_credentials_plugin* mk_metadata_client_plugin(
  haskell_get_metadata* f){

  //TODO: figure out when to free this.
  grpc_metadata_credentials_plugin* p =
    malloc(sizeof(grpc_metadata_credentials_plugin));

  p->get_metadata = metadata_dispatcher;
  p->destroy = NULL;
  p->state = f;
  p->type = "grpc-haskell custom credentials";

  return p;
}
