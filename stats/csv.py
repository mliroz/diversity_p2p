from stats.data import FigureGenerator


class CsvGenerator(FigureGenerator):

    def __init__(self, extension="csv"):
        self.extension = extension

    def draw_figure(self, figure_lines, out_name, title=None):

        out = open(out_name + "." + self.extension, "w")
        if title:
            out.write("# " + title + "\n")

        # Header and different x_values
        header = [figure_lines.x_var]
        x_values = set([])
        for idx, line_params in enumerate(figure_lines.lines_params):
            x_values_in_line = set([v[0] for v in figure_lines.lines_values[idx]])
            x_values = x_values.union(x_values_in_line)
            line_title = ", ".join(key + "=" + str(val)
                                   for key, val in zip(figure_lines.varying_keys, line_params))
            header.append('"' + line_title + '"')
        out.write(",".join(header) + "\n")

        # Points
        for x_val in sorted(x_values):
            point_y_values = []
            for values in figure_lines.lines_values:
                line_x_vals = [v[0] for v in values]
                if x_val in line_x_vals:
                    idx = line_x_vals.index(x_val)
                    point_y_values.append(values[idx][1])
                else:
                    point_y_values.append("")

            out.write(str(x_val) + "," + ",".join(str(p) for p in point_y_values) + "\n")

        # Non varying keys as comment
        assigs = []
        for key, value in zip(figure_lines.non_varying_keys,
                              figure_lines.non_varying_values):
            assigs.append(str(key) + " = " + str(value))
        out.write("# non_varying_params: " + ", ".join(assigs) + "\n")

        out.close()
