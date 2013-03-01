ckan.module('harvest-type-change', function (jQuery, _) {
  return {
    initialize: function () {
      var self, harvest_source_type;
      self = this;
      harvest_source_type = this.el.attr('value');
      this.el.change(function(){
        self.sandbox.publish('harvest-source-type-select', harvest_source_type);
      })
      if (this.el.attr("checked") === "checked"){
        self.sandbox.publish('harvest-source-type-select', harvest_source_type);
      }
    },
  }
})

ckan.module('harvest-extra-form-change', function (jQuery, _) {
  return {
    initialize: function () {
      var self, item, i, control_groups, control_group, item_name;
      self = this;
      self.sandbox.subscribe('harvest-source-type-select', function(source_type) {
        form_items = self.options.formItems;
        items = form_items[source_type] || [];

        control_groups = self.el.find('.control-group');
        for (i=0;i<control_groups.length;i++){
          control_group = $(control_groups[i])
          item_name = control_group.find('input').attr('name');
          if ($.inArray(item_name, items) === -1){
            control_group.hide();
          } else{
            control_group.show();
          }
        }
      })
    },
  }
})
