import celery
from celery import chord, group, chain
import random
import time
import logging

# --- Configuration & Constants ---

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BROKER_URL = 'redis://localhost:6379/0'
BACKEND_URL = 'redis://localhost:6379/0'

app = celery.Celery('tasks', broker=BROKER_URL, backend=BACKEND_URL)

# Keys
K_ORDER_ID = 'order_id'
K_PAYMENT_STATUS = 'payment_status'
K_PAYMENT_ID = 'payment_id'
K_INVENTORY_STATUS = 'inventory_status'
K_SHIPPING_STATUS = 'shipping_status'
K_PAYMENT_INFO = 'payment_info'
K_INVENTORY_INFO = 'inventory_info'
K_SOLD_ITEMS = 'sold_items'
K_ITEM_ID = 'item_id'
K_SELL_STATUS = 'sell_status'

# Values
STATUS_PROCESSED = 'processed'
STATUS_UPDATED = 'updated'
STATUS_LABEL_CREATED = 'label_created'
STATUS_COMPLETED = 'completed'

class OrderProcessingError(Exception):
    """Custom exception for order processing errors."""
    pass


# --- Helper Functions ---

def simulate_failure(probability=0.0):
    """Returns True if a failure should be simulated."""
    # Currently disabled in user code (if False), so set to 0.0 or False explicitly
    # Change to random.random() < probability to enable
    return False

def get_task_argument_dict(request, *args):
    """
    Tries to extract a dictionary of arguments from the task request or provided args.
    Handles standard task arguments and custom context passed via link_error.
    """
    # 1. Check extra args for explicit context (e.g. from link_error)
    for arg in args:
        if isinstance(arg, dict) and K_ORDER_ID in arg:
            return arg

    # 2. Check standard request arguments
    if request and request.args:
        # Usually the first argument is the payload from the previous task
        if isinstance(request.args[0], dict):
            return request.args[0]
            
    return {}

def extract_rollback_context(payload):
    """
    Parses a payload dictionary to extract relevant IDs and statuses for rollback.
    Handles nested structures (like payment_info inside inventory_result).
    """
    context = {
        K_ORDER_ID: payload.get(K_ORDER_ID),
        K_PAYMENT_ID: payload.get(K_PAYMENT_ID),
        K_PAYMENT_STATUS: payload.get(K_PAYMENT_STATUS),
        K_INVENTORY_STATUS: payload.get(K_INVENTORY_STATUS),
        K_SHIPPING_STATUS: payload.get(K_SHIPPING_STATUS)
    }

    # Dig deeper if payment info is missing but payment_info dict exists
    if not context[K_PAYMENT_ID] and K_PAYMENT_INFO in payload:
        p_info = payload[K_PAYMENT_INFO]
        if isinstance(p_info, dict):
            context[K_PAYMENT_ID] = p_info.get(K_PAYMENT_ID)
            context[K_PAYMENT_STATUS] = p_info.get(K_PAYMENT_STATUS)

    return context


# --- Core Tasks ---

@app.task(bind=True, max_retries=0, default_retry_delay=2)
def process_payment(self, order_id):
    """Processes the payment for an order."""
    logger.info(f"Processing payment for order {order_id}...")
    try:
        if simulate_failure():
            logger.warning(f"Payment processing failed for order {order_id}.")
            raise OrderProcessingError("Payment gateway timeout")
            
        logger.info(f"Payment processed successfully for order {order_id}")
        return {
            K_ORDER_ID: order_id, 
            K_PAYMENT_STATUS: STATUS_PROCESSED, 
            K_PAYMENT_ID: f'PAY-{order_id}'
        }
    except OrderProcessingError as exc:
        logger.warning(f"Retrying payment for order {order_id}...")
        self.retry(exc=exc)

@app.task(bind=True, max_retries=0, default_retry_delay=5)
def sell_item(self, order_id):
    """Sells a single item for an order."""
    logger.info(f"Selling item for order {order_id}...")
    try:
        time.sleep(1)
        if True: #random.random() < 0.1: #simulate_failure():
            logger.warning(f"Selling item failed for order {order_id}.")
            raise OrderProcessingError("Not in stock temporarily")
            
        logger.info(f"Item sold for order {order_id}.")
        return {
            K_ORDER_ID: order_id, 
            K_SELL_STATUS: STATUS_COMPLETED, 
            K_ITEM_ID: f'ITEM-{order_id}'
        }
    except OrderProcessingError as exc:
        logger.warning(f"Retrying selling item for order {order_id}...")
        self.retry(exc=exc)

@app.task(bind=True, max_retries=0, default_retry_delay=5)
def finish_inventory_update(self, sell_items_results, payment_result):
    """
    Callback task that runs after all items in the group are sold.
    """
    order_id = payment_result.get(K_ORDER_ID)
    
    # Extract item_ids for logging
    item_ids = [res.get(K_ITEM_ID) for res in sell_items_results if res]
    logger.info(f"All items sold for order {order_id}. Item IDs: {item_ids}")
    logger.info(f"Updating inventory for order {order_id} based on payment.")
    
    try:
        if simulate_failure():
             logger.error(f"Insufficient stock for order {order_id}. Cannot fulfill.")
             raise OrderProcessingError("Insufficient stock")

        logger.info(f"Inventory updated successfully for order {order_id}")
        return {
            K_ORDER_ID: order_id, 
            K_INVENTORY_STATUS: STATUS_UPDATED, 
            K_PAYMENT_INFO: payment_result, 
            K_SOLD_ITEMS: sell_items_results
        }
    except OrderProcessingError as exc:
        logger.warning(f"Retrying inventory update for order {order_id}...")
        self.retry(exc=exc)

@app.task(bind=True)
def update_inventory(self, payment_result):
    """
    Updates the inventory. 
    Triggers a group of sell_item tasks (parallel) and then finish_inventory_update (callback).
    """
    order_id = payment_result[K_ORDER_ID]
    items_count = 2 # Simulate items

    header = group(sell_item.s(order_id) for _ in range(items_count))
    callback = finish_inventory_update.s(payment_result)
    
    # CRITICAL: Attach error handlers to individual group tasks so failures in the group
    # propagate to the main error handler with context.
    err_handler = order_processing_error_handler.s(payment_result)
    for task_sig in header.tasks:
        task_sig.link_error(err_handler)
        # TODO: now for each sell_item task the refunds are done )
        # where the error for sell_item happens we should start the rollbacks once for the successful ones


    # replace() is used to swap this task with the chord, keeping the chain intact
    raise self.replace(chord(header, callback))

@app.task(bind=True, max_retries=3, default_retry_delay=5)
def create_shipping_label(self, inventory_result):
    """Creates a shipping label for an order."""
    order_id = inventory_result[K_ORDER_ID]
    logger.info(f"Creating shipping label for order {order_id}...")
    
    try:
        if simulate_failure():
            logger.warning(f"Shipping API is down for order {order_id}. Retrying...")
            raise OrderProcessingError("Shipping API unavailable")
            
        logger.info(f"Shipping label created successfully for order {order_id}")
        return {
            K_ORDER_ID: order_id, 
            K_SHIPPING_STATUS: STATUS_LABEL_CREATED, 
            K_INVENTORY_INFO: inventory_result
        }
    except OrderProcessingError as exc:
        self.retry(exc=exc)


# --- Workflow Finalization ---

@app.task
def notify_customer(results):
    """Notifies the customer that the order is complete."""
    # Extract deeply nested info for summary
    order_id = results.get(K_ORDER_ID)
    shipping_status = results.get(K_SHIPPING_STATUS)
    inventory_info = results.get(K_INVENTORY_INFO, {})
    payment_info = inventory_info.get(K_PAYMENT_INFO, {})
    payment_id = payment_info.get(K_PAYMENT_ID)
    sold_items = inventory_info.get(K_SOLD_ITEMS, [])
    item_ids = [item.get(K_ITEM_ID) for item in sold_items]

    logger.info("======================== Order Summary ========================")
    logger.info(f"Order Summary: OrderID={order_id}, PaymentID={payment_id}, Shipping={shipping_status}, ItemIDs={item_ids}")
    
    time.sleep(2) # Simulate email send
    logger.info(f"Customer notified for order {order_id}")
    return f"Customer notified for {order_id}"


# --- Rollback Tasks ---

@app.task
def refund_payment(payment_id):
    logger.info(f"Refunding payment {payment_id}...")
    time.sleep(1)
    logger.info(f"Payment refunded {payment_id}")
    return f"Payment refunded {payment_id}"

@app.task
def revert_inventory_update(order_id):
    logger.info(f"Reverting inventory for order {order_id}...")
    time.sleep(1)
    logger.info(f"Inventory reverted {order_id}")
    return f"Inventory reverted {order_id}"

@app.task
def cancel_shipping_label(order_id):
    logger.info(f"Canceling shipping label for order {order_id}...")
    time.sleep(1)
    logger.info(f"Shipping label canceled {order_id}")
    return f"Shipping label canceled {order_id}"


# --- Error Handling & Orchestration ---

@app.task
def order_processing_error_handler(*args):
    """
    Central error handler.
    Identifies failure context and orchestrates simpler compensating transactions (rollbacks).
    """
    logger.info("--- Order Processing Error Handler Invoked ---")
    
    # 1. Identify Context
    request_obj = None
    exception_obj = None
    
    for arg in args:
        if isinstance(arg, Exception):
            exception_obj = arg
        if hasattr(arg, 'id') and hasattr(arg, 'args'): # Celery Task Request object
            request_obj = arg

    if request_obj:
        logger.info(f"Task {request_obj.id} raised: {exception_obj}")

    # 2. Extract Business Data from Arguments
    # Checks both direct arguments (from link_error) and request.args (standard chain)
    payload_dict = get_task_argument_dict(request_obj, *args)
    ctx = extract_rollback_context(payload_dict)
    
    order_id = ctx.get(K_ORDER_ID)
    if not order_id:
        logger.error("Could not determine Order ID from error context. Skipping rollback.")
        return

    logger.error(f"!!! Order {order_id} failed: {exception_obj}")
    logger.info(f"Context: {ctx}")
    logger.info(f"--- Initiating Rollback for Order {order_id} ---")

    # 3. Execute Rollbacks based on status
    if ctx.get(K_PAYMENT_STATUS) == STATUS_PROCESSED:
        pid = ctx.get(K_PAYMENT_ID)
        if pid: refund_payment.delay(pid)

    if ctx.get(K_INVENTORY_STATUS) == STATUS_UPDATED:
        revert_inventory_update.delay(order_id)

    if ctx.get(K_SHIPPING_STATUS) == STATUS_LABEL_CREATED:
        cancel_shipping_label.delay(order_id)


@app.task
def process_order(order_id):
    """
    Orchestrator: Chains Payment -> Inventory -> Shipping.
    """
    workflow = chain(
        process_payment.s(order_id),
        update_inventory.s(),
        create_shipping_label.s()
    )

    workflow.apply_async(
        link=notify_customer.s(),
        link_error=order_processing_error_handler.s()
    )
