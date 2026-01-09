
import celery
from celery import chord
import random
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Celery app setup
app = celery.Celery('tasks', broker='redis://localhost:6379/0', backend='redis://localhost:6379/0')

class OrderProcessingError(Exception):
    """Custom exception for order processing errors."""
    pass

# --- Main Tasks ---

@app.task(bind=True, max_retries=0, default_retry_delay=2)
def process_payment(self, order_id):
    """Processes the payment for an order."""
    try:
        logging.info(f"Processing payment for order {order_id}...")
        # Simulate a transient failure
        if False: #random.choice([True, False, False]): # 33% chance of failure
            logging.warning(f"Payment processing failed for order {order_id}.")
            raise OrderProcessingError("Payment gateway timeout")
        logging.info(f"Payment processed successfully for order {order_id}")
        return {'order_id': order_id, 'payment_status': 'processed', 'payment_id': f'PAY-{order_id}'}
    except OrderProcessingError as exc:
        logging.warning(f"Retrying payment for order {order_id}...")
        self.retry(exc=exc)

@app.task(bind=True, max_retries=0, default_retry_delay=5)
def sell_item(self, order_id):
    """Sells the item for an order."""
    try:
        logging.info(f"Selling item for order {order_id}...")
        time.sleep(1)
        # Simulate a transient failure
        if False: #random.choice([True, False, False]): # 33% chance of failure
            logging.warning(f"Selling item failed for order {order_id}.")
            raise OrderProcessingError("Not in stock temporarily")
        else:
            logging.info(f"Item sold for order {order_id}.")

        return {'order_id': order_id, 'sell_status': 'completed', 'item_id': f'ITEM-{order_id}'}
    except OrderProcessingError as exc:
        logging.warning(f"Retrying selling item for order {order_id}...")
        self.retry(exc=exc)

@app.task(bind=True, max_retries=0, default_retry_delay=5)
def finish_inventory_update(self, sell_items_results, payment_result):
    """
    Callback task that runs after all items are sold.
    """
    order_id = payment_result['order_id']
    
    # Extract item_ids for logging
    item_ids = [res.get('item_id') for res in sell_items_results]
    logging.info(f"All items sold for order {order_id}. Item IDs: {item_ids}")

    logging.info(f"Updating inventory for order {order_id} based on payment: {payment_result}")
    
    try:
        # Simulate a permanent failure
        if False: #random.choice([True, False, False, False]): # 25% chance of failure
             logging.error(f"Insufficient stock for order {order_id}. Cannot fulfill.")
             raise OrderProcessingError("Insufficient stock")
        logging.info(f"Inventory updated successfully for order {order_id}")
        return {'order_id': order_id, 'inventory_status': 'updated', 'payment_info': payment_result, 'sold_items': sell_items_results}
    except OrderProcessingError as exc:
        logging.warning(f"Retrying inventory update for order {order_id}...")
        self.retry(exc=exc)

@app.task(bind=True)
def update_inventory(self, payment_result):
    """Updates the inventory for an order by triggering sell_item group."""
    order_id = payment_result['order_id']
    items = range(1)  # Simulate 4 items to be sold

    header = group(sell_item.s(order_id) for _ in items)
    # Ensure error handler is attached to the body of the chord (the callback)
    # However, if group tasks fail, the chord callback doesn't run.
    # We must propagate errors from the group to the main workflow error handler.
    callback = finish_inventory_update.s(payment_result)
    
    # We must also attach the error handler to the group itself, or construct the chord such that failures propagate.
    # A standard chord will NOT call the callback if a header task fails.
    # Using link_error on the group ensures the handler is called if any group task fails.
    for task_sig in header.tasks:
        task_sig.link_error(order_processing_error_handler.s(payment_result))

    raise self.replace(chord(header, callback))

@app.task(bind=True, max_retries=3, default_retry_delay=5)
def create_shipping_label(self, inventory_result):
    """Creates a shipping label for an order."""
    order_id = inventory_result['order_id']
    try:
        logging.info(f"Creating shipping label for order {order_id} based on inventory: {inventory_result}")
        # Simulate a transient failure
        if False: #random.choice([True, False, False]):
            logging.warning(f"Shipping API is down for order {order_id}. Retrying...")
            raise OrderProcessingError("Shipping API unavailable")
        logging.info(f"Shipping label created successfully for order {order_id}")
        return {'order_id': order_id, 'shipping_status': 'label_created', 'inventory_info': inventory_result}
    except OrderProcessingError as exc:
        self.retry(exc=exc)


# --- Callback Task ---

@app.task
def notify_customer(results):
    """Notifies the customer that the order is complete."""
    logging.info(f"All tasks completed for order: {results}")

    # Extract attributes for logging
    order_id = results.get('order_id')
    shipping_status = results.get('shipping_status')
    inventory_info = results.get('inventory_info', {})
    payment_info = inventory_info.get('payment_info', {})
    payment_id = payment_info.get('payment_id')
    sold_items = inventory_info.get('sold_items', [])
    item_ids = [item.get('item_id') for item in sold_items]

    logging.info("======================== Order Summary ========================")
    logging.info(f"Order Summary: OrderID={order_id}, PaymentID={payment_id}, Shipping={shipping_status}, ItemIDs={item_ids}")
    
    # Simulate sending an email
    time.sleep(2)
    logging.info(f"Customer notified for order {order_id}")
    return f"Customer notified for {order_id}"


# --- Compensating (Rollback) Tasks ---
@app.task
def refund_payment(payment_id):
    """Refunds the payment for a failed order."""
    logging.info(f"Refunding payment for payment ID {payment_id}...")
    time.sleep(1)
    logging.info(f"Payment refunded for payment ID {payment_id}")
    return f"Payment refunded for {payment_id}"

@app.task
def revert_inventory_update(order_id):
    """Reverts the inventory update for a failed order."""
    logging.info(f"Reverting inventory update for order {order_id}...")
    time.sleep(1)
    logging.info(f"Inventory reverted for order {order_id}")
    return f"Inventory reverted for {order_id}"

@app.task
def cancel_shipping_label(order_id):
    """Cancels the shipping label for a failed order."""
    logging.info(f"Canceling shipping label for order {order_id}...")
    time.sleep(1)
    logging.info(f"Shipping label canceled for order {order_id}")
    return f"Shipping label canceled for {order_id}"

# --- Error Handler Task ---
@app.task
def order_processing_error_handler(*args):
    """
    Handles errors during order processing and triggers rollbacks.
    Adapts to different signatures depending on context:
    - (request, exc, traceback): specific task failure
    - (context, request, exc, traceback): invoked via link_error with extra context
    """
    logging.info(f"--- Order Processing Error Handler Invoked ---")
    logging.info(f"Received {len(args)} arguments")
    
    # Attempt to identify arguments by type/structure
    payment_result_context = None
    request_context = None
    exception_obj = None
    
    # Helper to check if a dict is our payment_result
    def is_payment_result(d):
        return isinstance(d, dict) and 'payment_id' in d and 'order_id' in d

    for arg in args:
        if isinstance(arg, Exception):
            exception_obj = arg
        elif is_payment_result(arg):
            payment_result_context = arg
        elif hasattr(arg, 'argsrepr'): # Likely a Request/Context object
             request_context = arg

    # If we didn't find payment_result_context via direct dict check, 
    # check if it's hiding in the args of a Context object which might have been passed 
    # (though unlikely for bound args to end up INSIDE the request context's args)
    
    # Fallback assignment for standard signature (request, exc, traceback)
    if not request_context and len(args) >= 1 and hasattr(args[0], 'id'):
         request_context = args[0]
         if len(args) >= 2: exception_obj = args[1] 

    if request_context:
        logging.info(f"Task {request_context.id} raised: {exception_obj}")

    # Use the found payment_result
    if payment_result_context:
        order_id = payment_result_context.get('order_id')
        payment_id = payment_result_context.get('payment_id')
        logging.error(f"!!! Error handled with explicit context for order {order_id}")
        
        if payment_result_context.get('payment_status') == 'processed':
             logging.info(f"--- Initiating Rollback for Order {order_id} using explicit context ---")
             refund_payment.delay(payment_id)
        return

    # Access the actual arguments using request.args
    # argsrepr is just a string for display; args contains the real data (tuple)

    if request_context and request_context.args:
        task_args = request_context.args
        # logging.info(f"Full task arguments: {task_args}")

        # Based on your log, the dictionary is the first element of the tuple
        if len(task_args) > 0 and isinstance(task_args[0], dict):
            payload = task_args[0]
            order_id = payload.get('order_id')
            
            # Try to find payment details in top level or nested 'payment_info'
            payment_status = payload.get('payment_status')
            payment_id = payload.get('payment_id')
            
            if not payment_id and 'payment_info' in payload:
                payment_info = payload.get('payment_info', {})
                if isinstance(payment_info, dict):
                    payment_status = payment_info.get('payment_status')
                    payment_id = payment_info.get('payment_id')

            logging.error(f"!!! Order {order_id} failed: {exception_obj}")
            logging.info(f"Payment Status: {payment_status}, Payment ID: {payment_id}")
            logging.info(f"--- Initiating Rollback for Order {order_id} ---")
            
            # Perform Rollbacks based on available data
            if payment_status == 'processed':
                refund_payment.delay(payment_id)
            
            if payload.get('inventory_status') == 'updated':
                revert_inventory_update.delay(order_id)

            if payload.get('shipping_status') == 'label_created':
                cancel_shipping_label.delay(order_id)

    
from celery import chain, group

# --- Orchestrator ---

@app.task
def process_order(order_id):
    """
    Orchestrates the order processing workflow using a chain.
    The output of each task is passed as the first argument to the next.
    """
    # Define the chain of tasks
    workflow = chain(
        process_payment.s(order_id),
        update_inventory.s(),
        create_shipping_label.s()
    )

    # Execute the chain with a final callback for success and an error handler for failure
    workflow.apply_async(
        link=notify_customer.s(),
        link_error=order_processing_error_handler.s()
    )
