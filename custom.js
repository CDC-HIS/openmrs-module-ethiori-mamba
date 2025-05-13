jQuery(function () {
    // --- Initial Setup ---
    // Hide all elements intended as Gregorian targets and their default buttons.
    // Assuming Gregorian target inputs end with "GC" and their buttons have class ending with "GC smallButton".
    jQuery('input[id$="GC"], button[class$="GC smallButton"]').attr(
        "hidden",
        true,
    );

    // --- Dynamic Picker Initialization ---
    // Select all input fields that should become Ethiopian pickers.
    // IMPORTANT: In your HTML rendered by the custom widget,
    // add the class 'ethiopic-datepicker-input' and the data-gregorian-target attribute
    // to the input fields you want to be Ethiopian pickers.
    // Example HTML: <input type="text" class="ethiopic-datepicker-input" data-gregorian-target="userEnteredParamstartDateGC"/>
    jQuery(".ethiopic-datepicker-input").each(function () {
        var $ethiopicInput = jQuery(this);
        // Get the ID of the corresponding Gregorian input from the data attribute
        var gregorianTargetId = $ethiopicInput.data("gregorian-target");
        var $gregorianTargetInput = jQuery("#" + gregorianTargetId);

        // Basic check to ensure the target exists
        if ($gregorianTargetInput.length === 0) {
            console.error(
                "Gregorian target input with ID '" +
                gregorianTargetId +
                "' not found for:",
                $ethiopicInput.attr("id"),
            );
            return; // Skip this input if its target is missing
        }

        // Initialize the Ethiopian calendar picker for the current input
        $ethiopicInput.calendarsPicker({
            calendar: jQuery.calendars.instance("ethiopian", "am"), // Use Ethiopian calendar (Amharic locale)
            dateFormat: "yyyy-mm-dd", // Display format in the Ethiopian picker input (can adjust if needed)
            onSelect: function (ethdate) {
                // This function is called when a date is selected in the picker

                if (!ethdate) {
                    // If the date is cleared in the picker, clear the Gregorian target as well
                    $gregorianTargetInput.val("");
                    console.log("Date cleared. Gregorian target cleared.");
                    return;
                }

                try {
                    // Convert the selected Ethiopian date object to a Gregorian date object
                    var jd = ethdate.toJD(); // Convert Ethiopian date to Julian Day
                    var gregorianDate = jQuery.calendars.instance("gregorian").fromJD(jd); // Convert Julian Day to Gregorian date

                    // Format the Gregorian date as mm/dd/yyyy (or the format OpenMRS expects for Date parameters)
                    // mm/dd/yyyy is a common format parsed by many systems
                    var gregorianDateStr = jQuery.calendars
                        .instance("gregorian")
                        .formatDate("mm/dd/yyyy", gregorianDate);

                    // Set the value of the hidden Gregorian input field
                    // This is the value that the OpenMRS backend will receive
                    $gregorianTargetInput.val(gregorianDateStr);

                    console.log("Selected Ethiopian Date:", ethdate.format("yyyy-mm-dd")); // Log selected date for debugging
                    console.log(
                        "Converted Gregorian Date (mm/dd/yyyy):",
                        gregorianDateStr,
                    ); // Log converted date
                } catch (e) {
                    console.error("Error converting Ethiopian date:", e);
                    // Clear the target on error
                    $gregorianTargetInput.val("");
                }
            },
            // You can add other calendarsPicker options here (e.g., yearRange, minDate, maxDate)
        });
    });
});
