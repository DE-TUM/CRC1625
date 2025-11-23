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
            'label': 'data(label)',
            'color': '#000000',
            'text-valign': 'bottom',
            'text-margin-y': '5px',
            'text-halign': 'center',
            'font-size': '12px',
            'padding': '10px',
            'text-wrap': 'wrap',
            'background-color': '#0074D9', // Fallback default color
          }
        },

        {
          selector: 'node.step',
          style: {
            'background-color': '#FFBAA5',
          }
        },

        {
          selector: 'node.object',
          style: {
            'background-color': '#D5E8D4',
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
      }
    },

    addNode(id, label, type) {
      if (this.cy.$id(id).length === 0) {
        let classes = [];

        if (type && ['step', 'object'].includes(type.toLowerCase())) {
          classes.push(type.toLowerCase());
        }

        this.cy.add({
          group: 'nodes',
          data: { id: id, label: label },
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
  }
};