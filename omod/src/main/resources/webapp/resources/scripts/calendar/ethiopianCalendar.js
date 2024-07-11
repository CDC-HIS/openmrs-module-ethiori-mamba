
jQuery(function() {
 jQuery( "#userEnteredParamstartDateGC" ).attr('hidden',true);
 jQuery( "#userEnteredParamendDateGC" ).attr('hidden',true);
  jQuery( ".userEnteredParamstartDateGC.smallButton" ).attr('hidden',true);
  jQuery( ".userEnteredParamendDateGC.smallButton" ).attr('hidden',true);
  
  jQuery("#userEnteredParamstartDate").calendarsPicker({ 
	 onSelect: function ethToGreg(ethdate) {
	 if (!ethdate) return null;
   
	    var dmy = ethdate.toString().split("-");
	    console.log("splited date: "+dmy);
	    if (dmy.length == 3) {
		
		var appdate = jQuery.calendars.instance('ethiopian').newDate(parseInt(dmy[0], 10), parseInt(dmy[1], 10), parseInt(dmy[2], 10));
		var jd = jQuery.calendars.instance('ethiopian').toJD(appdate);
		var appdategc = jQuery.calendars.instance('gregorian').fromJD(jd);
		var appdategcstr = jQuery.calendars.instance('gregorian').formatDate('dd/mm/yyyy', appdategc);
		
		
		    jQuery("#userEnteredParamstartDateGC").val(appdategcstr);
	    
	    }

	    },
	calendar:jQuery.calendars.instance('ethiopian', 'am')});


 jQuery("#userEnteredParamendDate").calendarsPicker({ 
	 onSelect: function ethToGreg(ethdate) {
	 if (!ethdate) return null;
   
	    var dmy = ethdate.toString().split("-");
	    console.log("splited date: "+dmy);
	    if (dmy.length == 3) {
		
		var appdate = jQuery.calendars.instance('ethiopian').newDate(parseInt(dmy[0], 10), parseInt(dmy[1], 10), parseInt(dmy[2], 10));
		var jd = jQuery.calendars.instance('ethiopian').toJD(appdate);
		var appdategc = jQuery.calendars.instance('gregorian').fromJD(jd);
		var appdategcstr = jQuery.calendars.instance('gregorian').formatDate('dd/mm/yyyy', appdategc);
		
		
		    jQuery("#userEnteredParamendDateGC").val(appdategcstr);
		
	    }

	    },
	calendar:jQuery.calendars.instance('ethiopian', 'am')});
	
	jQuery("#userEnteredParamendDate").click(function() { 
			    jQuery('#userEnteredParamendDate').val('').calendarsPicker('option', 
				{dateFormat: 'dd/mm/yyyy'}); 
			});
	jQuery("#userEnteredParamstartDate").click(function() { 
	    jQuery('#userEnteredParamstartDate').val('').calendarsPicker('option', 
		{dateFormat: 'dd/mm/yyyy'}); 
	});
	
});
function showCalendar(obj, yearsPrevious) {
	// if the object doesn't have an id, just set it to some random text so jq can use it
	if(!obj.id) {
		obj.id = "something_random" + (Math.random()*1000);
	}
	
	//set appendText to something so it doesn't automagically pop into the page
	var opts = { appendText: " " };
	
	//set yearPrevious to 110 if it has'nt been parsed in an argument
	if (!yearsPrevious){
		yearsPrevious= 110;
	}
	opts["yearRange"] = "c-" + yearsPrevious + ":c30";
	
	if (gp.weekStart)
		opts["firstDay"] = gp.weekStart;
	
	//var dp = new DatePicker(jsDateFormat, obj.id, opts);
	//jQuery("#"+obj.id).calendarsPicker({
	 
	//calendar: $.calendars.instance('ethiopian', 'am')});
	//jQuery.datepicker.setDefaults(jQuery.datepicker.regional['et']);
	//obj.onclick = null;
	//dp.show();
	return false;
}

/**
 * Overload the showCalendar(obj, yearsPrevious) function for support
 * future dates in jQuery date picker 
 * 
 * @param HTMLInputElement obj 			 HTML element
 * @param integer		   yearsPrevious Backward year range
 * @param yearsNext		   yearsNext	 Forward year range
 * @returns {Boolean}
 */
function showCalendar(obj, yearsPrevious, yearsNext) {
	
	// if the object doesn't have an id, just set it to some random text so jq can use it
		/*if(!obj.id) {
			obj.id = "something_random" + (Math.random()*1000);
		}

		//set appendText to something so it doesn't automagically pop into the page
		var opts = { appendText: " " };
		
		//set yearPrevious to 110 if it has'nt been parsed in an argument
		if (!yearsPrevious){
			yearsPrevious= 110;
		}

		//set yearsNext to 10 if it has'nt been parsed in an argument
		if (!yearsNext){
			yearsNext= 20;
		}
		
		opts["yearRange"] = "-"+yearsPrevious+":+"+yearsNext;

		if (gp.weekStart)
			opts["firstDay"] = gp.weekStart;
		
		var dp = new DatePicker(jsDateFormat, obj.id, opts);
		jQuery.datepicker.setDefaults(jQuery.datepicker.regional['et']);
		obj.onclick = null;
		dp.show();
	*/
	/*jQuery("#"+obj.id).calendarsPicker({ 
	 onSelect: function ethToGreg(ethdate) {
	 if (!ethdate) return null;
   
	    var dmy = ethdate.toString().split("-");
	    console.log("splited date: "+dmy);
	    if (dmy.length == 3) {
		
		var appdate = jQuery.calendars.instance('ethiopian').newDate(parseInt(dmy[0], 10), parseInt(dmy[1], 10), parseInt(dmy[2], 10));
		var jd = jQuery.calendars.instance('ethiopian').toJD(appdate);
		var appdategc = jQuery.calendars.instance('gregorian').fromJD(jd);
		var appdategcstr = jQuery.calendars.instance('gregorian').formatDate('dd/mm/yyyy', appdategc);
		
		if(obj.id === "userEnteredParamstartDate"){
		    jQuery("#userEnteredParamstartDateGC").val(appdategcstr);
	    
		}else{
		    jQuery("#userEnteredParamendDateGC").val(appdategcstr);
		}
	    }

	    },
	calendar:jQuery.calendars.instance('ethiopian', 'am')});
	*/
	return false;
   
}

