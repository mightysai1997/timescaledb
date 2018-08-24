
#ifndef TIMESCALEDB_BGW_COUNTER_H
#define TIMESCALEDB_BGW_COUNTER_H

#include <postgres.h>



extern int	guc_max_background_workers;

extern void bgw_counter_shmem_alloc(void);
extern void bgw_counter_shmem_startup(void);

extern void bgw_counter_setup_gucs(void);

extern void bgw_counter_shmem_cleanup(void);
extern bool bgw_total_workers_increment(void);
extern void bgw_total_workers_decrement(void);
extern int	bgw_total_workers_get(void);


#endif							/* TIMESCALEDB_BGW_COUNTER_H */
