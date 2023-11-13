import os
import sys

import uvicorn
from fastapi import FastAPI
from starlette.responses import FileResponse

root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, root_path)
from etl.transform import Transform
from utils.logger import Logger
import etl.extract as extract
from startup import run
import utils.config as cfg

log = Logger("ServiceName")

app = FastAPI()


@app.post("/set_mongo")
async def set_mongo():
    data = extract.run()
    if data:
        try:
            run(data=data)
            return {'status': "success"}
        except Exception as e:
            return {'status': "failure", 'error': str(e)}
    else:
        log.warning(f'we have no records')
        return {'status': "empty", 'warning': f'we have no records'}


@app.post("/get_data")
async def get_data():
    data = extract.run()
    if data:
        transform = Transform()
        parsed_data = transform.run(data)
        for d in parsed_data:
            d['_id'] = str(d['_id'])
        return {"status": "success", "data": parsed_data}


@app.get("/")
async def read_index():
    return FileResponse(f'{root_path}/api/index.html')


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=10031)
