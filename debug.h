/* 
 * yubo@yubo.org
 * 2015-05-15
 */
#ifndef _DEBUG_H
#define _DEBUG_H
#include <sys/syslog.h>
#include <stdarg.h>
#include <stdio.h>

#define	L_EMERG    LOG_EMERG	/* system is unusable */
#define	L_ALERT    LOG_ALERT	/* action must be taken immediately */
#define	L_CRIT     LOG_CRIT		/* critical conditions */
#define	L_ERR      LOG_ERR		/* error conditions */
#define	L_WARNING  LOG_WARNING	/* warning conditions */
#define	L_NOTICE   LOG_NOTICE	/* normal but significant condition */
#define	L_INFO     LOG_INFO		/* informational */
#define	L_DEBUG    LOG_DEBUG	/* debug-level messages */

static const char *LOG_L[] = {
	[L_EMERG]   = "EMERG",
	[L_ALERT]   = "ALERT",
	[L_CRIT]    = "CRIT",
	[L_ERR]     = "ERR",
	[L_WARNING] = "WARNING",
	[L_NOTICE]  = "NOTICE",
	[L_INFO]    = "INFO" ,
	[L_DEBUG]   = "DEBUG" 
}; 


#ifdef _DEBUG
static inline void log_meesage(int level, const char *func, int line, const char*format, ...){
	if(_DEBUG >= level){
		char buf[512];
		va_list vl;

		snprintf(buf, sizeof(buf), "%-8s%s(%d): %s", LOG_L[level], func, line, format);
		va_start(vl, format);
#ifdef USE_SYSLOG
		vsyslog(level, buf, vl);
#else
		vfprintf(stderr, buf, vl);
#endif
		va_end(vl); 
	}
}
#define _DPRINT(level, format, ...) log_meesage(LOG_ ## level, __func__, __LINE__, format, ## __VA_ARGS__)
#define dlog(format, ...) _DPRINT(DEBUG, format, ## __VA_ARGS__)
#define elog(format, ...) _DPRINT(ERR, format, ## __VA_ARGS__)
#else
#define dlog(level, format, ...) 
#endif


#endif
