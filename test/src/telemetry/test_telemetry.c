#include <postgres.h>
#include <access/htup_details.h>
#include <utils/builtins.h>
#include <utils/jsonb.h>
#include <funcapi.h>
#include <fmgr.h>

#include "telemetry/telemetry.h"
#include "net/http.h"
#include "config.h"
#ifdef TS_DEBUG
#include "net/conn_mock.h"
#endif

#define HTTPS_PORT	443
#define TEST_ENDPOINT	"postman-echo.com"

TS_FUNCTION_INFO_V1(test_status);
TS_FUNCTION_INFO_V1(test_status_ssl);
TS_FUNCTION_INFO_V1(test_status_mock);
TS_FUNCTION_INFO_V1(test_telemetry);

#ifdef TS_DEBUG
static char *test_string;
#endif

static
HttpRequest *
build_request(int status)
{
	HttpRequest *req = http_request_create(HTTP_GET);
	char		uri[20];

	snprintf(uri, 20, "/status/%d", status);

	http_request_set_uri(req, uri);
	http_request_set_version(req, HTTP_VERSION_10);
	http_request_set_header(req, HTTP_HOST, TEST_ENDPOINT);
	http_request_set_header(req, HTTP_CONTENT_LENGTH, "0");
	return req;
}

static Datum
test_factory(ConnectionType type, int status, char *host, int port)
{
	Connection *conn;
	HttpRequest *req;
	HttpResponseState *rsp = NULL;
	HttpError	err;
	Datum		json;

	conn = connection_create(type);

	if (conn == NULL)
		return CStringGetTextDatum("could not initialize a connection");

	if (connection_connect(conn, host, NULL, port) < 0)
	{
		connection_destroy(conn);
		elog(ERROR, "connection error: %s", connection_get_and_clear_error(conn));
	}

#ifdef TS_DEBUG
	if (type == CONNECTION_MOCK)
		connection_mock_set_recv_buf(conn, test_string, strlen(test_string));
#endif

	req = build_request(status);

	rsp = http_response_state_create();

	err = http_send_and_recv(conn, req, rsp);

	http_request_destroy(req);
	connection_destroy(conn);

	if (err != HTTP_ERROR_NONE)
		elog(ERROR, "%s", http_strerror(err));

	if (!http_response_state_valid_status(rsp))
		elog(ERROR, "endpoint sent back unexpected HTTP status: %d",
			 http_response_state_status_code(rsp));

	json = DirectFunctionCall1(jsonb_in, CStringGetDatum(http_response_state_body_start(rsp)));

	http_response_state_destroy(rsp);

	return json;
}

/*  Test ssl_ops */
Datum
test_status_ssl(PG_FUNCTION_ARGS)
{
	int			status = PG_GETARG_INT32(0);
#ifdef TS_USE_OPENSSL

	return test_factory(CONNECTION_SSL, status, TEST_ENDPOINT, HTTPS_PORT);
#else
	char		buf[128] = {'\0'};

	if (status / 100 != 2)
		elog(ERROR, "endpoint sent back unexpected HTTP status: %d", status);

	snprintf(buf, sizeof(buf) - 1, "{\"status\":%d}", status);

	PG_RETURN_JSONB(DirectFunctionCall1(jsonb_in, CStringGetDatum(buf)));;
#endif
}

/*  Test default_ops */
Datum
test_status(PG_FUNCTION_ARGS)
{
	int			port = 80;
	int			status = PG_GETARG_INT32(0);

	PG_RETURN_JSONB(test_factory(CONNECTION_PLAIN, status, TEST_ENDPOINT, port));
}

#ifdef TS_DEBUG
/* Test mock_ops */
Datum
test_status_mock(PG_FUNCTION_ARGS)
{
	int			port = 80;
	text	   *arg1 = PG_GETARG_TEXT_P(0);

	test_string = text_to_cstring(arg1);

	PG_RETURN_JSONB(test_factory(CONNECTION_MOCK, 123, TEST_ENDPOINT, port));
}
#endif

TS_FUNCTION_INFO_V1(test_telemetry_parse_version);

Datum
test_telemetry_parse_version(PG_FUNCTION_ARGS)
{
	text	   *response = PG_GETARG_TEXT_P(0);
	long		installed_version[3] = {
		PG_GETARG_INT32(1),
		PG_GETARG_INT32(2),
		PG_GETARG_INT32(3),
	};
	VersionResult result = {0};
	TupleDesc	tupdesc;
	Datum		values[5];
	bool		nulls[5] = {false};
	HeapTuple	tuple;
	bool		success;

	if (PG_NARGS() != 4)
		PG_RETURN_NULL();

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in context "
						"that cannot accept type record")));

	success = telemetry_parse_version(text_to_cstring(response), installed_version, &result);

	if (!success)
		elog(ERROR, "%s", result.errhint);

	values[0] = CStringGetTextDatum(result.versionstr);
	values[1] = Int32GetDatum(result.version[0]);
	values[2] = Int32GetDatum(result.version[1]);
	values[3] = Int32GetDatum(result.version[2]);
	values[4] = BoolGetDatum(result.is_up_to_date);

	tuple = heap_form_tuple(tupdesc, values, nulls);

	return HeapTupleGetDatum(tuple);
}

Datum
test_telemetry(PG_FUNCTION_ARGS)
{
	Connection *conn;
	ConnectionType conntype;
	HttpRequest *req;
	HttpResponseState *rsp;
	HttpError	err;
	Datum		json_body;
	const char *host = PG_ARGISNULL(0) ? TELEMETRY_HOST : text_to_cstring(PG_GETARG_TEXT_P(0));
	const char *servname = PG_ARGISNULL(1) ? "https" : text_to_cstring(PG_GETARG_TEXT_P(1));
	int			port = PG_ARGISNULL(2) ? 0 : PG_GETARG_INT32(2);
	int			ret;

	if (PG_NARGS() > 3)
		elog(ERROR, "invalid number of arguments");

	if (strcmp("http", servname) == 0)
		conntype = CONNECTION_PLAIN;
	else if (strcmp("https", servname) == 0)
		conntype = CONNECTION_SSL;
	else
		elog(ERROR, "invalid service type '%s'", servname);

	conn = connection_create(conntype);

	if (conn == NULL)
		elog(ERROR, "could not create telemetry connection");

	ret = connection_connect(conn, host, servname, port);

	if (ret < 0)
	{
		const char *errstr = connection_get_and_clear_error(conn);

		connection_destroy(conn);

		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("could not make a connection to %s://%s", servname, host),
				 errdetail("%s", errstr)));
	}

	req = build_version_request(host, TELEMETRY_PATH);

	rsp = http_response_state_create();

	err = http_send_and_recv(conn, req, rsp);

	http_request_destroy(req);
	connection_destroy(conn);

	if (err != HTTP_ERROR_NONE)
		elog(ERROR, "HTTP error: %s", http_strerror(err));

	if (!http_response_state_valid_status(rsp))
		elog(ERROR, "invalid HTTP response status %d",
			 http_response_state_status_code(rsp));

	json_body = DirectFunctionCall1(jsonb_in, CStringGetDatum(http_response_state_body_start(rsp)));

	http_response_state_destroy(rsp);

	PG_RETURN_JSONB(json_body);
}
