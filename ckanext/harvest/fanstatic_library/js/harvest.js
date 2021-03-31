"use strict";

this.ckan.module('automatic-local-time', function (jQuery) {
  return {
    initialize: function () {
      var browserLocale = window.navigator.userLanguage || window.navigator.language;

      jQuery('.local-time').each(function() {
        moment.locale(browserLocale);
        var time = moment(jQuery(this).data('time'), "hh:mm A ZZ");
        if (time.isValid()) {
            jQuery(this).html(time.format("LT ([UTC]Z)"));
        }
        jQuery(this).show();
      })
    }
  }
})
