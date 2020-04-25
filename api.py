from flask import Flask, request
from flask_restful import Api, Resource, reqparse

from database import create_table, read_table, update_table

APP = Flask(__name__)
API = Api(APP)


class DataCreator(Resource):
    """
    Data Schema Creator
    """
    parser = reqparse.RequestParser()
    parser.add_argument('providerId', required=True, type=int)
    parser.add_argument('fields', required=True, type=dict)

    def post(self):
        args = self.parser.parse_args()
        provider_id = str(args["providerId"])
        fields = args['fields']
        status = create_table(table_name=provider_id, columns=fields)
        if status != "success":
            return status, 210
        return args, 201


class DataLoader(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('providerId', required=True, type=int)
    parser.add_argument('data', required=True, type=dict, action='append')

    def post(self):
        """
        Populates table with data
        """
        args = self.parser.parse_args()
        provider_id = str(args["providerId"])
        data = args['data']

        status = update_table(table_name=provider_id, data=data)
        if status != "success":
            return status, 211
        return args, 201


class DataRetriever(Resource):

    @staticmethod
    def construct_query_specification(queries):
        if len(queries) == 0:
            return None

        operators = {
            'eqc': 'ilike',
            'eq': '==',
            'lt': '<',
            'gt': '>'
        }
        spec = list()
        for query in queries:
            spec.append(dict())
            operator = operators.get(query[1].split(':')[0])
            spec[-1]['field'] = query[0]
            spec[-1]['op'] = operator
            spec[-1]['value'] = f"%{query[1].split(':')[1]}%" if operator == 'ilike' \
                else query[1].split(':')[1]
        return spec

    def get(self, provider_id):
        """
        Retrieves data from database table
        """
        queries = list(request.args.items())
        spec = self.construct_query_specification(queries)
        return read_table(table_name=str(provider_id), filter_spec=spec)


API.add_resource(DataCreator, '/create', endpoint='create')
API.add_resource(DataLoader, '/load', endpoint='load')
API.add_resource(DataRetriever, '/filter/<int:provider_id>',
                 endpoint='retrieve')


if __name__ == '__main__':
    # APP.run(debug=False)
    APP.run(debug=True)
