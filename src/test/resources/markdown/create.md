<# page template="/template/form.html?language=job&previous=任务列表&back=/job/tasks%3FjobId=%26(jobid)&button=任务列表&button_class=prime-button&crumb=批量创建任务&jump=/job/tasks%3FjobId=%26(jobId)" />

<%
VAR $job := SELECT title, job_type, cron_exp, enabled FROM qross_jobs WHERE id=#{jobId} -> FIRST ROW;
%>

<#include file="/job/rules.sql"/>

# # create-tasks-title #

/gray:# only-scheduled-job-tip #/

-- 20 --

<%
IF $job.job_type == 'scheduled' THEN
%>

/b:*# cron-exp # # global.required #/

/gray:# cron-exp-tip #/

<input id="CronExp" requried-text="" type="text" size="40" value="<%=$job.cron_exp%>" /> &nbsp; <button id="Verify" class="small-button task-incorrect-badge"># verify #</button>

-- 20 --

/b:*# start-time # # global.required #/

<a -href="/hello/world"></a>

/gray:# start-end-time-tip #/

<input id="StartTime" type="datetime" size="40" placeholder="yyyy-MM-dd HH:mm:00" readonly="true" <% IF @language == 'chinese' THEN %> calendar-lunar="yes" calendar-corner-names="休,班" calendar-week-title="周" calendar-week-names="一,二,三,四,五,六,日" calendar-month-names="一月,二月,三月,四月,五月,六月,七月,八月,九月,十月,十一月,十二月" calendar-this-month-text="本月" calendar-today-text="今天" calendar-head-format="yyyy年M月" <% END IF %> calendar-holiday="yes" calendar-min-date="<%=@NOW MINUS YEARS 3 FORMAT 'yyyy-MM-dd'%>" calendar-date="today" required-text="请选择开始时间。" clock-second-interval="0" />

-- 20 --

*# end-time # # global.required #

/gray:# start-end-time-tip #/

<input id="EndTime" type="datetime" size="40" placeholder="yyyy-MM-dd HH:mm:00" readonly="true" required-text="请选择结束时间。" />

-- 20 --

<button id="ListButton" color="prime"># list-tasks #</button></span>

<% END IF %>