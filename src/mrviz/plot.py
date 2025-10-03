import sys
import os
import math
import numpy as np
import pandas as pd
from bokeh.plotting import figure, show
from bokeh.layouts import column
from bokeh.models import ColumnDataSource

try:
    import reference.rosbag as rosbag
    from tf.transformations import euler_from_quaternion as efq
except ImportError:
    print("Rosbag or tf.transformations not available. Ensure ROS is installed.")
    sys.exit(1)

def read_fmcl_pose(bag_file):
    """
    Extract FMCL pose data from the bag file.
    """
    fmcl_t = []
    fmcl_x = []
    fmcl_y = []
    fmcl_theta = []

    with rosbag.Bag(bag_file) as bag:
        for topic, msg, t in bag.read_messages(topics=['/fmcl/pose']):
            fmcl_t.append(t.to_sec())
            fmcl_x.append(msg.pose.pose.position.x)
            fmcl_y.append(msg.pose.pose.position.y)
            z = msg.pose.pose.orientation.z
            w = msg.pose.pose.orientation.w
            fmcl_theta.append(efq([0, 0, z, w])[2])

    return pd.DataFrame({
        't': fmcl_t,
        'x': fmcl_x,
        'y': fmcl_y,
        'theta': fmcl_theta
    })

def plot_bag_file(bag_file):
    """
    Plot data from a bag file using Bokeh.
    """
    if not os.path.exists(bag_file):
        print(f"Bag file {bag_file} does not exist.")
        sys.exit(1)

    # Extract FMCL pose data
    fmcl_pose = read_fmcl_pose(bag_file)

    if not fmcl_pose.empty:
        p1 = figure(title="FMCL Pose", x_axis_label="X", y_axis_label="Y")
        p1.line(fmcl_pose['x'], fmcl_pose['y'], legend_label="Trajectory", line_width=2)

        # Show the plot
        show(column(p1))
    else:
        print("No FMCL pose data available to plot.")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: amrviz plot <bagfile.bag>")
        sys.exit(1)

    bag_file = sys.argv[1]
    plot_bag_file(bag_file)