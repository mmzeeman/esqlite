// This file is part of Emonk released under the MIT license. 
// See the LICENSE file for more information.

/* Adapted by: Maas-Maarten Zeeman <mmzeeman@xs4all.nl */

#include <assert.h>
#include <stdio.h>

#include "queue.h"

struct qitem_t
{
    struct qitem_t* next;
    void* data;
};

typedef struct qitem_t qitem;

struct queue_t
{
    ErlNifMutex *lock;
    ErlNifCond *cond;
    qitem *head;
    qitem *tail;
    void *message;
    int length;
};

queue *
queue_create()
{
    queue *ret;

    ret = (queue *) enif_alloc(sizeof(struct queue_t));
    if(ret == NULL) goto error;

    ret->lock = NULL;
    ret->cond = NULL;
    ret->head = NULL;
    ret->tail = NULL;
    ret->message = NULL;
    ret->length = 0;

    ret->lock = enif_mutex_create("queue_lock");
    if(ret->lock == NULL) goto error;
    
    ret->cond = enif_cond_create("queue_cond");
    if(ret->cond == NULL) goto error;

    return ret;

error:
    if(ret->lock != NULL) 
        enif_mutex_destroy(ret->lock);
    if(ret->cond != NULL) 
        enif_cond_destroy(ret->cond);
    if(ret != NULL) 
        enif_free(ret);
    return NULL;
}

void
queue_destroy(queue *queue)
{
    ErlNifMutex *lock;
    ErlNifCond *cond;
    int length;

    enif_mutex_lock(queue->lock);
    lock = queue->lock;
    cond = queue->cond;
    length = queue->length;

    queue->lock = NULL;
    queue->cond = NULL;
    queue->head = NULL;
    queue->tail = NULL;
    queue->length = -1;
    enif_mutex_unlock(lock);

    assert(length == 0 && "Attempting to destroy a non-empty queue.");
    enif_cond_destroy(cond);
    enif_mutex_destroy(lock);
    enif_free(queue);
}

int
queue_has_item(queue *queue)
{
    int ret;

    enif_mutex_lock(queue->lock);
    ret = (queue->head != NULL);
    enif_mutex_unlock(queue->lock);
    
    return ret;
}

int
queue_push(queue *queue, void *item)
{
    qitem * entry = (qitem *) enif_alloc(sizeof(struct qitem_t));
    if(entry == NULL) 
        return 0;

    entry->data = item;
    entry->next = NULL;

    enif_mutex_lock(queue->lock);

    assert(queue->length >= 0 && "Invalid queue size at push");
    
    if(queue->tail != NULL)
        queue->tail->next = entry;

    queue->tail = entry;

    if(queue->head == NULL)
        queue->head = queue->tail;

    queue->length += 1;

    enif_cond_signal(queue->cond);
    enif_mutex_unlock(queue->lock);

    return 1;
}

void*
queue_pop(queue *queue)
{
    qitem *entry;
    void* item;

    enif_mutex_lock(queue->lock);
    
    /* Wait for an item to become available.
     */
    while(queue->head == NULL)
        enif_cond_wait(queue->cond, queue->lock);
    
    assert(queue->length >= 0 && "Invalid queue size at pop.");

    /* Woke up because queue->head != NULL
     * Remove the entry and return the payload.
     */
    entry = queue->head;
    queue->head = entry->next;
    entry->next = NULL;

    if(queue->head == NULL) {
        assert(queue->tail == entry && "Invalid queue state: Bad tail.");
        queue->tail = NULL;
    }

    queue->length -= 1;

    enif_mutex_unlock(queue->lock);

    item = entry->data;
    enif_free(entry);

    return item;
}

int
queue_send(queue *queue, void *item)
{
    enif_mutex_lock(queue->lock);
    assert(queue->message == NULL && "Attempting to send multiple messages.");
    queue->message = item;
    enif_cond_signal(queue->cond);
    enif_mutex_unlock(queue->lock);
    return 1;
}

void *
queue_receive(queue *queue)
{
    void *item;

    enif_mutex_lock(queue->lock);
    
    /* Wait for an item to become available.
     */
    while(queue->message == NULL)
        enif_cond_wait(queue->cond, queue->lock);

    item = queue->message;
    queue->message = NULL;
    
    enif_mutex_unlock(queue->lock);
    
    return item;
}
