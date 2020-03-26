from dlflow.utils.datetools import trans_date

from string import Template
import re


DT_PATTERN = re.compile(r"(<dt=(.+?):(.+?)>)")


class ConfigTemplate(Template):

    def __init__(self, template):
        super(ConfigTemplate, self).__init__(template)
        self._vars = None

    @property
    def vars(self):
        if self._vars is None:
            self._vars = self.get_vars()

        return self._vars

    def get_vars(self):
        return list({
            var
            for pattern_values in self.pattern.findall(self.template)
            for var in pattern_values
            if var})

    def render(self, *args, **kwargs):
        render_str = self.substitute(*args, **kwargs)

        for src, dt, fmt in DT_PATTERN.findall(render_str):
            fmt_dt = trans_date(dt, fmt)
            render_str = render_str.replace(src, fmt_dt)

        return render_str
