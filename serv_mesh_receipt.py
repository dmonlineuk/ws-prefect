from mesh_receipt import mesh_receipt
from prefect import flow
import os


if __name__ == '__main__':
    # os.environ['PREFECT_API_URL'] = 'http://127.0.0.1:4200/api'
    servable_flow = flow(mesh_receipt)
    servable_flow.serve(
            name="Mesh Receipt",
            cron="*/15 * * * *"
        )
