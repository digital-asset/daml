#ifndef GRPC_HASKELL
#define GRPC_HASKELL

#include <grpc/grpc.h>
#include <grpc/grpc_security.h>
#include <grpc/impl/codegen/slice.h>
#include <grpc/support/time.h>
#include <grpc/byte_buffer.h>
#include <grpc/byte_buffer_reader.h>

grpc_event *grpc_completion_queue_next_(grpc_completion_queue *cq,
                                       gpr_timespec *deadline,
                                       void *reserved);

grpc_event *grpc_completion_queue_pluck_(grpc_completion_queue *cq, void *tag,
                                        gpr_timespec *deadline,
                                        void *reserved);

grpc_call *grpc_channel_create_call_(grpc_channel *channel,
                                     grpc_call *parent_call,
                                     uint32_t propagation_mask,
                                     grpc_completion_queue *completion_queue,
                                     const char *method, const char *host,
                                     gpr_timespec *deadline, void *reserved);

grpc_slice* grpc_slice_malloc_(size_t len);

size_t grpc_slice_length_(grpc_slice *slice);

uint8_t *grpc_slice_start_(grpc_slice *slice);

grpc_slice* grpc_slice_from_copied_string_(const char* source);

grpc_slice* grpc_slice_from_copied_buffer_(const char *source, size_t len);

void free_slice(grpc_slice *slice);

grpc_byte_buffer **create_receiving_byte_buffer();

void destroy_receiving_byte_buffer(grpc_byte_buffer **bb);

grpc_byte_buffer_reader *byte_buffer_reader_create(grpc_byte_buffer *buffer);

void byte_buffer_reader_destroy(grpc_byte_buffer_reader *reader);

grpc_slice* grpc_byte_buffer_reader_readall_(grpc_byte_buffer_reader *reader);

void timespec_destroy(gpr_timespec* t);

gpr_timespec* gpr_inf_future_(gpr_clock_type t);

gpr_timespec* gpr_now_(gpr_clock_type t);

int32_t gpr_time_to_millis_(gpr_timespec* t);

gpr_timespec* seconds_to_deadline(int64_t seconds);

gpr_timespec* millis_to_deadline(int64_t millis);

gpr_timespec* infinite_deadline();

gpr_timespec* convert_clock_type(gpr_timespec *t, gpr_clock_type to);

grpc_metadata_array** metadata_array_create();

void metadata_array_destroy(grpc_metadata_array **arr);

grpc_metadata* metadata_alloc(size_t n);

void metadata_free(grpc_metadata* m);

void set_metadata_key_val(char *key, char *val, size_t val_len,
                          grpc_metadata *arr, size_t i);

grpc_slice* get_metadata_key(grpc_metadata *arr, size_t i);

grpc_slice* get_metadata_val(grpc_metadata *arr, size_t i);

grpc_op* op_array_create(size_t n);

void op_array_destroy(grpc_op* op_array, size_t n);

void op_send_initial_metadata(grpc_op *op_array, size_t i,
                              grpc_metadata *arr, size_t n_metadata);

void op_send_initial_metadata_empty(grpc_op *op_array, size_t i);

void op_send_message(grpc_op *op_array, size_t i,
                     grpc_byte_buffer *payload);

void op_send_close_client(grpc_op *op_array, size_t i);

void op_recv_initial_metadata(grpc_op *op_array, size_t i,
                              grpc_metadata_array** arr);

void op_recv_message(grpc_op *op_array, size_t i,
                     grpc_byte_buffer **payload_recv);

void op_recv_status_client(grpc_op *op_array, size_t i,
                          grpc_metadata_array** arr,
                          grpc_status_code* status,
                          grpc_slice* details);

void op_recv_close_server(grpc_op *op_array, size_t i, int *was_cancelled);

void op_send_status_server(grpc_op *op_array, size_t i,
                           size_t metadata_count, grpc_metadata* m,
                           grpc_status_code status, grpc_slice *details);

grpc_status_code* create_status_code_ptr();

grpc_status_code deref_status_code_ptr(grpc_status_code* p);

void destroy_status_code_ptr(grpc_status_code* p);

grpc_call_details* create_call_details();

void destroy_call_details(grpc_call_details* cd);

void grpc_channel_watch_connectivity_state_(grpc_channel *channel,
                                            grpc_connectivity_state
                                            last_observed_state,
                                            gpr_timespec* deadline,
                                            grpc_completion_queue *cq,
                                            void *tag);

grpc_metadata* metadata_array_get_metadata(grpc_metadata_array* arr);

void metadata_array_set_metadata(grpc_metadata_array* arr, grpc_metadata* meta);

size_t metadata_array_get_count(grpc_metadata_array* arr);

size_t metadata_array_get_capacity(grpc_metadata_array* arr);

grpc_call* grpc_channel_create_registered_call_(
  grpc_channel *channel, grpc_call *parent_call, uint32_t propagation_mask,
  grpc_completion_queue *completion_queue, void *registered_call_handle,
  gpr_timespec *deadline, void *reserved);

grpc_slice* call_details_get_method(grpc_call_details* details);

grpc_slice* call_details_get_host(grpc_call_details* details);

gpr_timespec* call_details_get_deadline(grpc_call_details* details);

void* grpc_server_register_method_(
  grpc_server* server, const char* method, const char* host,
  grpc_server_register_method_payload_handling payload_handling);

//c2hs doesn't support #const pragmas referring to #define'd strings, so we use
//this enum as a workaround. These are converted into actual GRPC #defines in
// translate_arg_key in grpc_haskell.c.
enum supported_arg_key {
  compression_algorithm_key = 0,
  compression_level_key,
  user_agent_prefix_key,
  user_agent_suffix_key,
  max_receive_message_length_key,
  max_metadata_size_key,
};

grpc_arg* create_arg_array(size_t n);

void create_string_arg(grpc_arg* args, size_t i,
                       enum supported_arg_key key, char* value);

void create_int_arg(grpc_arg* args, size_t i,
                    enum supported_arg_key key, int value);

void destroy_arg_array(grpc_arg* args, size_t n);

grpc_auth_property_iterator* grpc_auth_context_property_iterator_(
  const grpc_auth_context* ctx);

grpc_server_credentials* ssl_server_credentials_create_internal(
  const char* pem_root_certs, const char* pem_key, const char* pem_cert,
  grpc_ssl_client_certificate_request_type force_client_auth);

grpc_channel_credentials* grpc_ssl_credentials_create_internal(
  const char* pem_root_certs, const char* pem_key, const char* pem_cert);

void grpc_server_credentials_set_auth_metadata_processor_(
  grpc_server_credentials* creds, grpc_auth_metadata_processor* p);

//packs a Haskell server-side auth processor function pointer into the
//appropriate struct expected by gRPC.
grpc_auth_metadata_processor* mk_auth_metadata_processor(
  void (*process)(void *state, grpc_auth_context *context,
                  const grpc_metadata *md, size_t num_md,
                  grpc_process_auth_metadata_done_cb cb, void *user_data));

grpc_call_credentials* grpc_metadata_credentials_create_from_plugin_(
  grpc_metadata_credentials_plugin* plugin);

//type of the callback used to create auth metadata on the client
typedef void (*get_metadata)
       (void *state, grpc_auth_metadata_context context,
        grpc_credentials_plugin_metadata_cb cb, void *user_data);

//type of the Haskell callback that we use to create auth metadata on the client
typedef void haskell_get_metadata(grpc_auth_metadata_context*,
                                  grpc_credentials_plugin_metadata_cb,
                                  void*);

grpc_metadata_credentials_plugin* mk_metadata_client_plugin(
  haskell_get_metadata* f);

#endif //GRPC_HASKELL
