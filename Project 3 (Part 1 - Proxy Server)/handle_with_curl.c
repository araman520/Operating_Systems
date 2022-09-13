#include "gfserver.h"
#include "proxy-student.h"
#include <sys/stat.h>
#include <curl/curl.h>
#include <stdio.h>

#define BUFSIZE (6201)

static size_t header_callback(void *ptr, size_t size, size_t nitems, void *ctx)
{

	size_t ds = nitems * size;

	return ds;
}

static size_t send_data(void *ptr, size_t size, size_t nitems, void *ctx)
{
	
	size_t sent = gfs_send((gfcontext_t *)ctx, ptr, size * nitems);

	return sent;
}

/*
 * Replace with an implementation of handle_with_curl and any other
 * functions you may need.
 */
ssize_t handle_with_curl(gfcontext_t *ctx, const char *path, void* arg)
{
	char buffer[BUFSIZE];
	char *data_dir = arg;

	strncpy(buffer, data_dir, BUFSIZE);
	strncat(buffer, path, BUFSIZE);

	CURL *curl_handle;
	CURLcode res;

	printf("Path = %s\n", buffer);

	curl_global_init(CURL_GLOBAL_ALL);

	curl_handle = curl_easy_init();

	curl_easy_setopt(curl_handle, CURLOPT_URL, buffer);

	curl_easy_setopt(curl_handle, CURLOPT_NOBODY, 1L); //Do not get body

	curl_easy_setopt(curl_handle, CURLOPT_HEADERFUNCTION, header_callback);

	curl_easy_setopt(curl_handle, CURLOPT_FAILONERROR, 1);

	res = curl_easy_perform(curl_handle);

	size_t dResult;

	curl_easy_getinfo(curl_handle, CURLINFO_CONTENT_LENGTH_DOWNLOAD_T, &dResult);

	printf("Code = %d\n", res);

	if(res == CURLE_HTTP_RETURNED_ERROR) 
	{
		gfs_sendheader(ctx, GF_FILE_NOT_FOUND, 0);
		return 0;
	}

	else if(res != CURLE_OK)
	{
		gfs_sendheader(ctx, GF_ERROR, 0);
		return -1;
	}

	gfs_sendheader(ctx, GF_OK, dResult);

	curl_easy_cleanup(curl_handle);

	curl_handle = curl_easy_init();

	curl_easy_setopt(curl_handle, CURLOPT_URL, buffer);

	curl_easy_setopt(curl_handle, CURLOPT_WRITEFUNCTION, send_data);

	curl_easy_setopt(curl_handle, CURLOPT_WRITEDATA, ctx);

	curl_easy_setopt(curl_handle, CURLOPT_FAILONERROR, 1);
	
	res = curl_easy_perform(curl_handle);

	if(res != CURLE_OK) return -1;

	curl_easy_cleanup(curl_handle);

	printf("\n\n\n\n");

	return 0;

	
}


/*
 * We provide a dummy version of handle_with_file that invokes handle_with_curl
 * as a convenience for linking.  We recommend you simply modify the proxy to
 * call handle_with_curl directly.
 */
ssize_t handle_with_file(gfcontext_t *ctx, const char *path, void* arg){
	return handle_with_curl(ctx, path, arg);
}	