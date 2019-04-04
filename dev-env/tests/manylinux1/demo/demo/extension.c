// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Copyright (c) 2019, Digital Asset (Switzerland) GmbH and/or its affiliates.
// All rights reserved.

#include "Python.h"
#include <glib.h>

static PyObject * py_isotime(PyObject *self, PyObject *args) {
  GTimeVal now;
  g_get_current_time(&now);
  gchar *now_iso = g_time_val_to_iso8601(&now);
  return PyUnicode_FromFormat("The current time is: %s", now_iso);
}

static PyMethodDef module_functions[] = {
	{"isotime",  py_isotime, METH_VARARGS, NULL},
	{NULL, NULL}      /* sentinel */
};


static struct PyModuleDef moduledef = {
    PyModuleDef_HEAD_INIT,
    "ext",               /* m_name */
    "This is a module",  /* m_doc */
    -1,                  /* m_size */
    module_functions,    /* m_methods */
    NULL,                /* m_reload */
    NULL,                /* m_traverse */
    NULL,                /* m_clear */
    NULL,                /* m_free */
};


PyMODINIT_FUNC PyInit_ext(void) {
  return PyModule_Create(&moduledef);
}
