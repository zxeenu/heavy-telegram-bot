from src.app_context import AppContext


def main(ctx: AppContext):
    ctx.logger.info("Service started")
    ctx.channel.queue_declare(queue='task_queue', durable=True)
    ctx.channel.basic_publish(
        exchange='', routing_key='task_queue', body='hello')
    ctx.logger.info("Message sent")


if __name__ == '__main__':
    ctx = AppContext()
    try:
        main(ctx)
    finally:
        ctx.close()
