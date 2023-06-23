import json
import duckdb
import falcon
class QuoteResource:

    def __init__(self):
        self.con = duckdb.connect()

    def execute_sql(self, sql, table, return_fmt):
        renderred_sql = sql.replace("local_parquet", table)
        res = self.con.sql(renderred_sql)
        if return_fmt == "numpy":
            res_obj = res.fetchnumpy()
            for key in res_obj.keys():
                res_obj[key] = res_obj[key].tolist()
            return res_obj
        if return_fmt == "arrow":
            return json.loads(res.fetch_arrow_table().to_pandas().to_json())
        if return_fmt == "dataframe":
            return json.loads(res.fetchdf().to_json())
        if return_fmt == "polar":
            return res.pl().to_dicts()
        return res

    def on_post(self, req, resp):
        """Handles GET requests"""
        req_obj = json.load(req.bounded_stream)
        if 'return_fmt' not in req_obj or req_obj['return_fmt'] not in ['numpy', "arrow", "dataframe", "polar"]:
            req_obj['return_fmt'] = 'polar'
        res_obj = self.execute_sql(req_obj['sql'], req_obj['table'], req_obj['return_fmt'])
        resp.media = {"data":res_obj}



app = falcon.App()
app.add_route('/sql', QuoteResource())

# to start the server
# please use:  gunicorn -b :38000 cduckdb:app
if __name__ == "__main__":
    from waitress import serve
    serve(app, host='0.0.0.0', port=8000)
