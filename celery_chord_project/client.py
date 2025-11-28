
from tasks import process_order
import uuid

def main():
    """
    Triggers the order processing workflow for a new order.
    """
    order_id = str(uuid.uuid4())
    print(f"Starting order processing for order_id: {order_id}")
    process_order.delay(order_id)
    print("Order processing workflow initiated.")

if __name__ == '__main__':
    main()
