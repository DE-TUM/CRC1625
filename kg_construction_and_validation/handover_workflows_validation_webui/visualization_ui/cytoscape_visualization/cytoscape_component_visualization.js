export default {
  template: `
    <div style="position: relative; width: 100%; height: 500px;">
      <div ref="cy" style="width: 100%; height: 100%; display: block; border: 1px solid #ddd; border-radius: 8px;"></div>
      <div v-if="hoverData" 
           :style="{ 
             position: 'absolute', 
             top: hoverPos.y + 'px', 
             left: hoverPos.x + 'px', 
             background: '#333', 
             color: '#fff', 
             padding: '8px', 
             borderRadius: '4px', 
             fontSize: '12px', 
             zIndex: 10, 
             pointerEvents: 'none',
             transform: 'translate(-50%, -120%)',
             whiteSpace: 'pre-wrap'
           }">
        <strong>Validation result:</strong> {{ hoverData }}
      </div>
    </div>
  `,

  props: {
    nodes: Array,
    edges: Array,
  },

  data() {
    return {
      hoverData: null,
      hoverPos: { x: 0, y: 0 }
    };
  },

  mounted() {
    this.cy = cytoscape({
      container: this.$refs.cy,

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
              if (projects.length === 0) return `${label}\n${activityText}`;
              if (projects.length === 1) return `${label}\n${activityText}\n\nProject ${projects[0]}`;

              return `${label}\n${activityText}\n\nProjects\n${projects.join(',\n')}`;
            },
            'background-color': (ele) => ele.data('color') || '#0074D9',
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
            'border-width': 2,
            'border-color': '#5898d4',
            'font-weight': 'bold',
          }
        },

        // Node coloring according to validation results
        {
          selector: '.valid_step',
          style: {
            'background-color': '#369c4e',
            'background-image': [
              '/assets/check_circle.svg',
            ],
            'background-fit': 'contain',
            'background-image-opacity': 0.5,
            'background-image-containment': 'inside'
          }
        },
        {
          selector: '.invalid_step',
          style: {
            'background-color': '#d40820',
            'background-image': [
              '/assets/error.svg',
            ],
            'background-fit': 'contain',
            'background-image-opacity': 0.5,
            'background-image-containment': 'inside'
          }
        },
        { // The step had missing handovers
          selector: '.missing_step',
          style: {
            'background-color': '#e88b00',
            'background-image': [
              '/assets/error.svg',
            ],
            'background-fit': 'contain',
            'background-image-opacity': 0.5,
            'background-image-containment': 'inside'
          }
        },
        { // Any and all steps after a "missing handovers" step
          selector: '.not_checked_step',
          style: {
            'background-color': '#454549',
            'background-image': [
              '/assets/error.svg',
            ],
            'background-fit': 'contain',
            'background-image-opacity': 0.5,
            'background-image-containment': 'inside'
          }
        },
        {
          selector: 'edge',
          style: {
            'width': 2,
            'line-color': '#37648f',
            'curve-style': 'round-taxi',
            'target-arrow-shape': 'triangle',
            'target-arrow-color': '#37648f',
            'source-arrow-color': '#37648f',
            'arrow-scale': 1.2,
          }
        }
      ],
      layout: {
        name: 'dagre',
        fit: true,
        padding: 10,
        rankDir: 'LR',
        //animate: true
        rankSep: 100,
        nodeSep: 125
      }
    });

    // Tooltip listeners
    const validationSelector = 'node.valid_step, node.invalid_step, node.missing_step, node.not_checked_step';

    this.cy.on('mouseover', validationSelector, (evt) => {
      const node = evt.target;
      const msg = node.data('validationTooltip');
      if (msg) {
        this.hoverData = msg;
        this.updateTooltipPos(evt);
      }
    });

    this.cy.on('mousemove', validationSelector, (evt) => {
      if (this.hoverData) this.updateTooltipPos(evt);
    });

    this.cy.on('mouseout', validationSelector, () => {
      this.hoverData = null;
    });

    // Selection listeners
    this.cy.on('tap', 'node', (evt) => {
      const node = evt.target;
      this.cy.elements().removeClass('selected');
      node.addClass('selected');
      this.$emit('nodeClick', { id: node.id(), label: node.data('label') });
    });

    this.cy.on('tap', (evt) => {
      if (evt.target === this.cy) this.cy.elements().removeClass('selected');
    });

    this.rerun_layout_and_fit();
  },

  methods: {
    updateTooltipPos(evt) {
      const pos = evt.renderedPosition;
      this.hoverPos = { x: pos.x, y: pos.y };
    },

    rerun_layout_and_fit() {
      // Fit to the graph elements with a small padding
      this.cy.layout({ name: 'dagre', fit: true, padding: 10, rankDir: 'LR', animate: true, rankSep: 100, nodeSep: 125 }).run();
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
      return this.cy.edges(`[source = "${source}"][target = "${target}"]`).length > 0;
    },

    removeEdge(source, target) {
      const edge = this.cy.edges(`[source = "${source}"][target = "${target}"]`);

      if (edge.length > 0) {
        edge.remove();
        this.rerun_layout_and_fit();
      }
    },

    renameNode(id, newLabel) {
      // We only change the node's label to avoid recreating the whole
      // cytoscape data structure again
      const node = this.cy.$id(id);
      node.data('label', newLabel);

      // Force a re-render of the label
      node.trigger('data');
    },

    addNode(id, label, type, node_color) {
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
      if (node.length > 0) node.addClass('selected');
    },

    clearValidationResults() {
      this.cy.elements().removeClass('valid_step');
      this.cy.elements().removeClass('invalid_step');
      this.cy.elements().removeClass('missing_step');
      
      // Clear stored tooltips
      this.cy.nodes().removeData('validationTooltip');
    },

    setNodeAsValid(id, tooltip = 'Valid') {
      const node = this.cy.$id(id);
      if (node.length > 0) {
        node.addClass('valid_step');
        node.data('validationTooltip', tooltip);
      }
    },
    
    setNodeAsInvalid(id, tooltip = 'Invalid') {
      const node = this.cy.$id(id);
      if (node.length > 0) {
        node.addClass('invalid_step');
        node.data('validationTooltip', tooltip);
      }
    },
    
    setNodeAsMissing(id, tooltip = 'Missing') {
      const node = this.cy.$id(id);
      if (node.length > 0) {
        node.addClass('missing_step');
        node.data('validationTooltip', tooltip);
      }
    },

    setNodeAsNotChecked(id, tooltip = 'Not checked') {
      const node = this.cy.$id(id);
      if (node.length > 0) {
        node.addClass('not_checked_step');
        node.data('validationTooltip', tooltip);
      }
    },

    addActivity(id, activity, new_node_color) {
      const node = this.cy.$id(id);
      if (node.length > 0) {
        const activities = node.data('activities') || [];
        if (!activities.includes(activity)) {
          node.data('activities', [...activities, activity]);
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
        const filtered = activities.filter(a => a !== activity);
        node.data('activities', filtered);
        node.trigger('data');
        this.rerun_layout_and_fit();
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