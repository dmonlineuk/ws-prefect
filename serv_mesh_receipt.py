from mesh_receipt import mesh_receipt
from prefect import flow


if __name__ == '__main__':
    servable_flow = flow(mesh_receipt)
    servable_flow.serve(
            name="Mesh Receipt",
            cron="*/15 * * * *"
        )
