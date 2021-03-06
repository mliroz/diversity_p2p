import os
from stats.data import FigureGenerator

from subprocess import call

#GNUPLOT_HOME = r'C:\Program Files (x86)\gnuplot\bin\gnuplot.exe'
GNUPLOT_HOME = "/usr/bin/gnuplot"


class GnuPlotGenerator(FigureGenerator):

    def __init__(self, term="pngcairo dashed", extension="png"):
        self.header = "# File automatically generated by stats.data.GnuPlotGenerator\n" \
                      "set terminal " + term + "\n"
        self.extension = extension

    def draw_figure(self, figure_lines, out_name, title=None):

        script_dir = out_name
        os.makedirs(script_dir)
        script_file = os.path.join(script_dir, out_name + ".gpi")
        data_files = []

        script_out = open(script_file, "w")
        script_out.write(self.header)
        script_out.write("set output '" + out_name + "." + self.extension + "'\n")

        if title:
            script_out.write("set title '" + title + "'\n")
        script_out.write("set xlabel '" + figure_lines.x_var + "'\n")
        script_out.write("set ylabel '" + figure_lines.y_var + "'\n")

        plot_lines = []
        for idx, line_params in enumerate(figure_lines.lines_params):

            # Create data file
            data_file = os.path.join(script_dir, "line-" + str(idx) + ".dat")

            data_files.append(data_file)
            data_out = open(data_file, "w")
            data_out.write("# File automatically generated by stats.data.GnuPlotGenerator\n")
            for row in figure_lines.lines_values[idx]:
                data_out.write(" ".join(str(x) for x in row) + "\n")
            data_out.close()

            line_title = ", ".join(key + "=" + str(val)
                                   for key, val in zip(figure_lines.varying_keys, line_params))

            plot_lines.append("'" + data_file + "' u 1:2 w lp title \"" + line_title + "\"")

        script_out.write("plot " + ",\\\n".join(plot_lines) + "\n")
        script_out.close()

        call([GNUPLOT_HOME,script_file])