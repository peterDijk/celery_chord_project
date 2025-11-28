
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

@app.task(bind=True, max_retries=3, default_retry_delay=5)
def process_payment(self, order_id):
    """Processes the payment for an order."""
    try:
        logging.info(f"Processing payment for order {order_id}...")
        # Simulate a transient failure
        if random.choice([True, False, False]): # 33% chance of failure
            logging.warning(f"Payment processing failed for order {order_id}. Retrying...")
            raise OrderProcessingError("Payment gateway timeout")
        logging.info(f"Payment processed successfully for order {order_id}")
        return f"Payment successful for {order_id}"
    except OrderProcessingError as exc:
        self.retry(exc=exc)

@app.task(bind=True, max_retries=3, default_retry_delay=5)
def update_inventory(self, order_id):
    """Updates the inventory for an order."""
    try:
        logging.info(f"Updating inventory for order {order_id}...")
        # Simulate a permanent failure
        if random.choice([True, False, False, False]): # 25% chance of failure
             logging.error(f"Insufficient stock for order {order_id}. Cannot fulfill.")
             raise OrderProcessingError("Insufficient stock")
        logging.info(f"Inventory updated successfully for order {order_id}")
        return f"Inventory updated for {order_id}"
    except OrderProcessingError as exc:
        self.retry(exc=exc)

@app.task(bind=True, max_retries=3, default_retry_delay=5)
def create_shipping_label(self, order_id):
    """Creates a shipping label for an order."""
    try:
        logging.info(f"Creating shipping label for order {order_id}...")
        # Simulate a transient failure
        if random.choice([True, False, False]):
            logging.warning(f"Shipping API is down for order {order_id}. Retrying...")
            raise OrderProcessingError("Shipping API unavailable")
        logging.info(f"Shipping label created successfully for order {order_id}")
        return f"Shipping label created for {order_id}"
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
def order_processing_error_handler(request, exc, traceback, order_id, successful_tasks):
    """
    Handles errors during order processing and triggers rollbacks.
    """
    logging.error(f"!!! Order {order_id} failed: {exc}")
    logging.info(f"--- Initiating Rollback for Order {order_id} ---")

    for task_name in successful_tasks:
        if task_name == process_payment.name:
            refund_payment.delay(order_id)
        elif task_name == update_inventory.name:
            revert_inventory_update.delay(order_id)
        elif task_name == create_shipping_label.name:
            cancel_shipping_label.delay(order_id)

# --- Orchestrator ---

@app.task
def process_order(order_id):
    """
    Orchestrates the order processing workflow using a chord.
    """
    header = [
        process_payment.s(order_id),
        update_inventory.s(order_id),
        create_shipping_label.s(order_id),
    ]

    # Create a mutable list to track successful tasks
    successful_tasks = []

    # Link error callbacks to each task in the header
    for task_signature in header:
        task_signature.set(
            link_error=order_processing_error_handler.s(order_id, successful_tasks)
        )

    # The chord's callback will only execute if all header tasks succeed
    callback = notify_customer.s(order_id)

    # Before each task runs, add its name to the successful_tasks list
    # This is a bit of a workaround to track which tasks have completed
    # successfully before a failure in the chord. A more robust solution
    # might involve a state machine or a more complex tracking mechanism
    # in a database.
    @celery.signals.before_task_publish.connect
    def update_sent_tasks(sender=None, headers=None, body=None, **kwargs):
        if headers['task'] in [t.name for t in header]:
            if headers['task'] not in successful_tasks:
                successful_tasks.append(headers['task'])

    chord(header)(callback)
