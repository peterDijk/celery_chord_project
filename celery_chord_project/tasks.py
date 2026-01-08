
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
        if True: #random.choice([True, False, False]): # 33% chance of failure
            logging.warning(f"Payment processing failed for order {order_id}.")
            raise OrderProcessingError("Payment gateway timeout")
        logging.info(f"Payment processed successfully for order {order_id}")
        return {'order_id': order_id, 'payment_status': 'processed', 'payment_id': f'PAY-{order_id}'}
    except OrderProcessingError as exc:
        logging.warning(f"Retrying payment for order {order_id}...")
        self.retry(exc=exc)

@app.task(bind=True, max_retries=3, default_retry_delay=5)
def sell_item(self, order_id):
    """Sells the item for an order."""
    try:
        logging.info(f"Selling item for order {order_id}...")
        time.sleep(1)
        logging.info(f"Item sold for order {order_id}.")
        # Simulate a transient failure
        if random.choice([True, False, False]): # 33% chance of failure
            logging.warning(f"Selling item failed for order {order_id}.")
            raise OrderProcessingError("Inventory system timeout")
        logging.info(f"Item sold successfully for order {order_id}")
        return {'order_id': order_id, 'sell_status': 'completed'}
    except OrderProcessingError as exc:
        logging.warning(f"Retrying selling item for order {order_id}...")
        self.retry(exc=exc)

@app.task(bind=True, max_retries=3, default_retry_delay=5)
def update_inventory(self, payment_result):
    """Updates the inventory for an order."""
    order_id = payment_result['order_id']
    items = range(4)  # Simulate 4 items to be sold

    try:
        logging.info(f"Updating inventory for order {order_id} based on payment: {payment_result}")
        # Simulate a permanent failure
        if random.choice([True, False, False, False]): # 25% chance of failure
             logging.error(f"Insufficient stock for order {order_id}. Cannot fulfill.")
             raise OrderProcessingError("Insufficient stock")
        logging.info(f"Inventory updated successfully for order {order_id}")
        return {'order_id': order_id, 'inventory_status': 'updated', 'payment_info': payment_result}
    except OrderProcessingError as exc:
        logging.warning(f"Retrying inventory update for order {order_id}...")
        self.retry(exc=exc)

@app.task(bind=True, max_retries=3, default_retry_delay=5)
def create_shipping_label(self, inventory_result):
    """Creates a shipping label for an order."""
    order_id = inventory_result['order_id']
    try:
        logging.info(f"Creating shipping label for order {order_id} based on inventory: {inventory_result}")
        # Simulate a transient failure
        if random.choice([True, False, False]):
            logging.warning(f"Shipping API is down for order {order_id}. Retrying...")
            raise OrderProcessingError("Shipping API unavailable")
        logging.info(f"Shipping label created successfully for order {order_id}")
        return {'order_id': order_id, 'shipping_status': 'label_created', 'inventory_info': inventory_result}
    except OrderProcessingError as exc:
        self.retry(exc=exc)


# --- Callback Task ---

@app.task
def notify_customer(results, order_id):
    """Notifies the customer that the order is complete."""
    logging.info(f"All tasks completed for order {order_id}: {results}")
    logging.info(f"Notifying customer for order {order_id}...")
    # Simulate sending an email
    time.sleep(2)
    logging.info(f"Customer notified for order {order_id}")
    return f"Customer notified for {order_id}"


# --- Compensating (Rollback) Tasks ---
@app.task
def refund_payment(order_id):
    """Refunds the payment for a failed order."""
    logging.info(f"Refunding payment for order {order_id}...")
    time.sleep(1)
    logging.info(f"Payment refunded for order {order_id}")
    return f"Payment refunded for {order_id}"

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
def order_processing_error_handler(task_id):
    """
    Handles errors during order processing and triggers rollbacks.
    """
    logging.info(f"--- Order Processing Error Handler Invoked ---")

    result = celery.result.AsyncResult(task_id)

    order_id = result.args[0] if result.args else 'unknown'
    exc = result.result

    logging.info(f"result: {result}")

    # logging.error(f"!!! Order {order_id} failed: {exc}")
    # logging.info(f"--- Initiating Rollback for Order {order_id} ---")

    # logging.info(f"Successful tasks before failure: {successful_tasks}")

    # for task_name in successful_tasks:
    #     if task_name == process_payment.name:
    #         refund_payment.delay(order_id)
    #     elif task_name == update_inventory.name:
    #         revert_inventory_update.delay(order_id)
    #     elif task_name == create_shipping_label.name:
    #         cancel_shipping_label.delay(order_id)

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
