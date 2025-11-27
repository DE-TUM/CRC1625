export default {
  template: `
    <div style="width: 100%; height: 500px; display: block; border: 1px solid #ddd; border-radius: 8px;"></div>
  `,

  props: {
    nodes: Array,
    edges: Array,
  },

  mounted() {
    this.cy = cytoscape({
      container: this.$el,

      elements: {
        nodes: this.nodes,
        edges: this.edges,
      },

      style: [
        // Default node style
        {
          selector: 'node',
          style: {
            'label': (ele) => {
              const label = ele.data('label') || '';
              const activities = ele.data('activities') || [];

              if (activities.length === 0) {
                return label;
              }

              const activityText = '\n'+activities.join('\n');

              const projects = ele.data('projects') || "";
              if (projects.length === 0) {
                return `${label}\n${activityText}`;
              } else if (projects.length === 1) {
                console.log("1:", projects);
                return `${label}\n${activityText}\n\nProject ${projects[0]}`;
              } else {
                console.log(">1:", projects);
                return `${label}\n${activityText}\n\nProjects\n${projects.join(',\n')}`;
              }

            },
            'background-color': (ele) => {
              return ele.data('color') || '#0074D9';
            },
            'color': '#000000',
            'text-valign': 'bottom',
            'text-margin-y': '5px',
            'text-halign': 'center',
            'font-size': '12px',
            'padding': '10px',
            'text-wrap': 'wrap',
          }
        },

        {
          selector: 'node.step',
          style: {
          }
        },

        {
          selector: 'node.object',
          style: {
            'shape': 'round-rectangle',
          }
        },

        {
          selector: 'node.invisible',
          style: {
            'width': 0,
            'height': 0,
            'padding': 0,
            'background-opacity': 0,
            'border-width': 0,

            'label': '',
            'text-opacity': 0,
          }
        },

        // Selected nodes (applied on top of step/object styles)
        {
          selector: '.selected',
          style: {
            'border-width': 4,
            'border-color': '#FF4136',
            'font-weight': 'bold',
          }
        },

        // Edges
        {
          selector: 'edge',
          style: {
            'width': 2,
            'line-color': '#ccc',
            'target-arrow-shape': 'delta',
            'target-arrow-color': '#999',
            'curve-style': 'bezier'
          }
        }
      ],
      layout: {
        name: 'dagre',
        fit: true,
        padding: 10,
        rankDir: 'LR',
        //animate: true
      }
    });

    /*
    Event listeners
     */

    // Clicking a node
    this.cy.on('tap', 'node', (evt) => {
      const node = evt.target;

      this.cy.elements().removeClass('selected');
      node.addClass('selected');

      this.$emit('nodeClick', { id: node.id(), label: node.data('label') });
    });

    // Clicking the background (deselect)
    this.cy.on('tap', (evt) => {
      if (evt.target === this.cy) {
        this.cy.elements().removeClass('selected');
      }
    });

    this.rerun_layout_and_fit();
  },

  // Javascript methods that will be hooked into python in cytoscape_component.py
  methods: {
    // --- Existing methods ---

    rerun_layout_and_fit() {
      // Fit to the graph elements with a small padding
      this.cy.layout({ name: 'dagre', fit: true, padding: 10, rankDir: 'LR', animate: true }).run();
      this.cy.resize();
      this.cy.fit(this.cy.elements(), 10);
    },

    addEdge(source, target) {
      this.cy.add({
        group: 'edges',
        data: { source: source, target: target }
      });

      this.rerun_layout_and_fit();
    },

    existsEdge(source, target) {
      const edge = this.cy.edges(`[source = "${source}"][target = "${target}"]`);

      return edge.length > 0;
    },

    removeEdge(source, target) {
      const edge = this.cy.edges(`[source = "${source}"][target = "${target}"]`);

      if (edge.length > 0) {
        edge.remove();
        this.rerun_layout_and_fit();
      }
    },

    renameNode(id, newLabel) {
      const node = this.cy.$id(id);
      if (node.length > 0) {
        node.data('label', newLabel);

        // Force a re-render of the label
        node.trigger('data');
      }
    },

    addNode(id, label, type, node_color) { // <-- node_color is used here
      if (this.cy.$id(id).length === 0) {
        let classes = [];

        if (type && ['step', 'object'].includes(type.toLowerCase())) {
          classes.push(type.toLowerCase());
        }

        this.cy.add({
          group: 'nodes',
          data: {
            id: id,
            label: label,
            activities: [],
            color: node_color
          },
          classes: classes
        });

        this.rerun_layout_and_fit();
      }
    },

    removeNode(node_id) {
      const node = this.cy.getElementById(node_id);

      if (node.length > 0) {
        node.remove();
        this.rerun_layout_and_fit();
      }
    },

    selectNode(id) {
      this.cy.elements().removeClass('selected');
      const node = this.cy.$id(id);
      if (node.length > 0) {
        node.addClass('selected');
      }
    },

    addActivity(id, activity, new_node_color) {
      const node = this.cy.$id(id);
      if (node.length > 0) {
        const activities = node.data('activities') || [];
        if (!activities.includes(activity)) {

          const newActivities = [...activities, activity];
          node.data('activities', newActivities);

          // Force a re-render of the label and its color
          node.data('color', new_node_color);
          node.trigger('data');
          this.rerun_layout_and_fit();
        }
      }
    },

    removeActivity(id, activity) {
      const node = this.cy.$id(id);
      if (node.length > 0) {
        const activities = node.data('activities') || [];
        const index = activities.indexOf(activity);

        if (index > -1) {
          const newActivities = activities.filter(a => a !== activity);
          node.data('activities', newActivities);

          // Force a re-render of the label and its color
          node.data('color', new_node_color);
          node.trigger('data');
          this.rerun_layout_and_fit();
        }
      }
    },

    replaceActivities(id, activities, new_node_color) {
      const node = this.cy.$id(id);
      if (node.length > 0) {
        node.data('activities', activities);

        // Force a re-render of the label and its color
        node.data('color', new_node_color);
        node.trigger('data');
        this.rerun_layout_and_fit();
      }
    },

    replaceProjects(id, projects) {
      const node = this.cy.$id(id);
      if (node.length > 0) {
        node.data('projects', projects);

        // Force a re-render of the label and its color
        node.trigger('data');
        this.rerun_layout_and_fit();
      }
    }
  }
};