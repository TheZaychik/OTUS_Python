import abc
import datetime
import hashlib
import json
import logging
import re
import uuid
from http.server import BaseHTTPRequestHandler, HTTPServer
from optparse import OptionParser
from typing import Any

from scoring import get_interests, get_score

SALT = "Otus"
ADMIN_LOGIN = "admin"
ADMIN_SALT = "42"
OK = 200
BAD_REQUEST = 400
FORBIDDEN = 403
NOT_FOUND = 404
INVALID_REQUEST = 422
INTERNAL_ERROR = 500
ERRORS = {
    BAD_REQUEST: "Bad Request",
    FORBIDDEN: "Forbidden",
    NOT_FOUND: "Not Found",
    INVALID_REQUEST: "Invalid Request",
    INTERNAL_ERROR: "Internal Server Error",
}
UNKNOWN = 0
MALE = 1
FEMALE = 2
GENDERS = {
    UNKNOWN: "unknown",
    MALE: "male",
    FEMALE: "female",
}


class BaseField:
    def __init__(self, required, nullable=False) -> None:
        self.required = required
        self.nullable = nullable

    def __set_name__(self, owner, name) -> None:
        self.name = name

    def __get__(self, instance, owner) -> Any:
        return instance.__dict__[self.name]

    def __set__(self, instance, value) -> None:
        if not value and self.nullable is False:
            raise ValueError(f"Non-nullable {self.name} field cannot be None")
        instance.__dict__[self.name] = value

    def __delete__(self, instance) -> None:
        if self.required:
            raise Exception(f"Cannot delete required field {self.name}")
        del instance.__dict__[self.name]


class BaseModel:
    def __init__(self, **kwargs) -> None:
        user_class_attrs = {
            key: value
            for key, value in self.__class__.__dict__.items()
            if issubclass(value.__class__, BaseField)
        }

        for field_name, descr in user_class_attrs.items():
            if field_name not in kwargs.keys():
                if descr.required:
                    raise ValueError(f"Required field {field_name} must be specified")
            else:
                kw_value = kwargs.get(field_name)

                if not descr.nullable and kw_value is None:
                    raise Exception(f"Non-nullable {field_name} field cannot be empty")

                setattr(self, field_name, kw_value)


class CharField(BaseField):
    def __set__(self, instance, value) -> None:
        if not isinstance(value, (str | None)):
            raise ValueError(f"Field {self.name} must be a string")
        if value and self.required and value == "":
            raise ValueError(f"Field {self.name} required")
        super().__set__(instance, value)


class ArgumentsField(BaseField):
    def __set__(self, instance, value) -> None:
        if not isinstance(value, (dict | None)):
            raise ValueError(f"Field {self.name} must be a dict")
        super().__set__(instance, value)


class EmailField(BaseField):
    def __set__(self, instance, value) -> None:
        if not isinstance(value, (str | None)):
            raise ValueError(f"Field {self.name} must be a string")
        if value and len(re.findall(r"^\S+@\S+\.\S+$", value)) == 0:
            raise ValueError("Email validation failed")
        super().__set__(instance, value)


class PhoneField(BaseField):
    def __set__(self, instance, value) -> None:
        if not isinstance(value, (str | int | None)):
            raise ValueError(f"Field {self.name} must be a string or int")
        else:
            if isinstance(value, int):
                value = str(value)
            if value and len(re.findall(r"^7[0-9]{10}$", value)) == 0:
                raise ValueError("PhoneNumber validation failed")
        super().__set__(instance, value)


class DateField(BaseField):
    def __set__(self, instance, value) -> None:
        if not isinstance(value, (str | None)):
            raise ValueError(f"Field {self.name} must be a string")
        if value and len(re.findall(r"^[0-9]{1,2}\.[0-9]{1,2}\.[0-9]{4}$", value)) == 0:
            raise ValueError(f"Field {self.name} validation failed")
        super().__set__(instance, value)


class BirthDayField(DateField):
    def __set__(self, instance, value) -> None:
        super().__set__(instance, value)
        date = datetime.datetime.strptime(value, "%d.%m.%Y")
        if datetime.datetime.now().year - date.year > 70:
            raise ValueError(f"Field {self.name}, value {value} is bigger than 70")


class GenderField(BaseField):
    def __set__(self, instance, value) -> None:
        if not isinstance(value, (int | None)):
            raise ValueError(f"Field {self.name} must be a int")
        if value and value not in (0, 1, 2):
            raise ValueError(f"Field {self.name} must be a 0, 1, 2")
        super().__set__(instance, value)


class ClientIDsField(BaseField):
    def __init__(self, required) -> None:
        super().__init__(required)

    def __set__(self, instance, value) -> None:
        if not isinstance(value, list) or not all(isinstance(i, int) for i in value):
            raise ValueError(f"Field {self.name} must be a list[int]")

        super().__set__(instance, value)


class ClientsInterestsRequest(BaseModel):
    client_ids = ClientIDsField(required=True)
    date = DateField(required=False, nullable=True)


class OnlineScoreRequest(BaseModel):
    first_name = CharField(required=False, nullable=True)
    last_name = CharField(required=False, nullable=True)
    email = EmailField(required=False, nullable=True)
    phone = PhoneField(required=False, nullable=True)
    birthday = BirthDayField(required=False, nullable=True)
    gender = GenderField(required=False, nullable=True)

    def __init__(self, **kwargs) -> None:
        pair_exists = self._check_pairs(**kwargs)
        print(kwargs, pair_exists)
        if not pair_exists:
            raise ValueError(f"No one pair")
        super().__init__(**kwargs)

    @staticmethod
    def _check_pairs(**kwargs) -> bool:
        pairs = [
            {"phone", "email"},
            {"first_name", "last_name"},
            {"birthday", "gender"},
        ]
        for pair in pairs:
            if len(pair & set(kwargs.keys())) == 2:
                return True
        return False


class MethodRequest(BaseModel):
    account = CharField(required=False, nullable=True)
    login = CharField(required=True, nullable=True)
    token = CharField(required=True, nullable=True)
    arguments = ArgumentsField(required=True, nullable=True)
    method = CharField(required=True)

    @property
    def is_admin(self):
        return self.login == ADMIN_LOGIN


def check_auth(request):
    if request.is_admin:
        digest = hashlib.sha512(
            (datetime.datetime.now().strftime("%Y%m%d%H") + ADMIN_SALT).encode()
        ).hexdigest()
    else:
        digest = hashlib.sha512(
            (request.account + request.login + SALT).encode()
        ).hexdigest()
    if digest == request.token:
        return True
    return False


def method_handler(request, ctx, store):
    response, code = None, None
    print(request)
    body = request.get("body")
    if body is None or not isinstance(body, dict) or len(body.keys()) == 0:
        return response, INVALID_REQUEST
    method = HandlerClass.__dict__.get(body.get("method"))
    if method:
        response, code = method(request, ctx, store)
        return response, code
    return {"error": "Method not found"}, INVALID_REQUEST


class HandlerClass:
    @staticmethod
    def online_score(request, ctx, store) -> (dict, int):
        try:
            req = MethodRequest(**request["body"])
        except ValueError as e:
            return {"error": str(e)}, INVALID_REQUEST

        if not check_auth(req):
            return {"error": "403 Forbidden"}, FORBIDDEN

        if req.is_admin:
            return {"score": 42}, OK

        try:
            args = OnlineScoreRequest(**(request["body"]["arguments"]))
            ctx["has"] = request["body"]["arguments"]
            score = get_score(
                store,
                args.__dict__.get("phone"),
                args.__dict__.get("email"),
                args.__dict__.get("birthday"),
                args.__dict__.get("gender"),
                args.__dict__.get("first_name"),
                args.__dict__.get("last_name"),
            )

            return {"score": score}, OK
        except (ValueError, KeyError) as e:
            return {"error": str(e)}, INVALID_REQUEST

    @staticmethod
    def clients_interests(request, ctx, store):
        try:
            req = MethodRequest(**request["body"])
        except ValueError as e:
            return {"error": str(e)}, INVALID_REQUEST

        if not check_auth(req):
            return {"error": "403 Forbidden"}, FORBIDDEN

        try:
            args = ClientsInterestsRequest(**(request["body"]["arguments"]))
            response = {cid: get_interests(store, cid) for cid in args.client_ids}
            ctx["nclients"] = len(args.client_ids)
            return response, OK
        except (ValueError, KeyError) as e:
            return {"error": str(e)}, INVALID_REQUEST


class MainHTTPHandler(BaseHTTPRequestHandler):
    router = {"method": method_handler}
    store = None

    def get_request_id(self, headers):
        return headers.get("HTTP_X_REQUEST_ID", uuid.uuid4().hex)

    def do_POST(self):
        response, code = {}, OK
        context = {"request_id": self.get_request_id(self.headers)}
        request = None
        try:
            data_string = self.rfile.read(int(self.headers["Content-Length"]))
            request = json.loads(data_string)
        except:
            code = BAD_REQUEST

        if request:
            path = self.path.strip("/")
            logging.info("%s: %s %s" % (self.path, data_string, context["request_id"]))
            if path in self.router:
                try:
                    response, code = self.router[path](
                        {"body": request, "headers": self.headers}, context, self.store
                    )
                except Exception as e:
                    logging.exception("Unexpected error: %s" % e)
                    code = INTERNAL_ERROR
            else:
                code = NOT_FOUND

        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        if code not in ERRORS:
            r = {"response": response, "code": code}
        else:
            r = {"error": response or ERRORS.get(code, "Unknown Error"), "code": code}
        context.update(r)
        logging.info(context)
        self.wfile.write(json.dumps(r))
        return


if __name__ == "__main__":
    op = OptionParser()
    op.add_option("-p", "--port", action="store", type=int, default=8080)
    op.add_option("-l", "--log", action="store", default=None)
    (opts, args) = op.parse_args()
    logging.basicConfig(
        filename=opts.log,
        level=logging.INFO,
        format="[%(asctime)s] %(levelname).1s %(message)s",
        datefmt="%Y.%m.%d %H:%M:%S",
    )
    server = HTTPServer(("localhost", opts.port), MainHTTPHandler)
    logging.info("Starting server at %s" % opts.port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    server.server_close()
