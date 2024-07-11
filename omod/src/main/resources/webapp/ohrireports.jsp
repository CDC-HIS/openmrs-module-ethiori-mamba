<%@ include file="/WEB-INF/template/include.jsp"%>

<%@ include file="/WEB-INF/template/header.jsp"%>
<%@ include file="/WEB-INF/view/module/reporting/includeScripts.jsp"%>
<h2><spring:message code="ohrireports.title" /></h2>

<br/>
<table>
  <tr>
   <th>User Id</th>
   <th>Username</th>
  </tr>
  <c:forEach var="user" items="${users}">
      <tr>
        <td>${user.userId}</td>
        <td>${user.systemId}</td>
      </tr>		
  </c:forEach>
</table>

<%@ include file="/WEB-INF/template/footer.jsp"%>
