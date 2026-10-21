# Visualisation UI Documentation
This document provides an overall view of the visualisation user interface, aka folder visualization.ui.
The relevant contents of the folder are:
- Cytoscape visualization files: javascript file with custom cytoscape layout and a python file that functions as a wrapper for the javascript, as the majority of visualisation code and its logic are written in python, as nobody could be bothered with working in pure js.
- Queries folder: contains all queries that are used in the workflow visualisation. To learn more about them go directly to the queries files and read their descriptions.
- graph specs and adapter files: these contain logic to help transform retrieved data into a format that works for cytoscape layout.
- Visualization file: handles the landing page and transfers between samples.

The next section of this document will be dealing with architecture of each file mentioned above, and a bit more in depth insight on what's going on in there. Though, the explanation of each function logic will be handled directly in the respective file as in-line comments/documentation comments.

TODO: write the rest of the markdown